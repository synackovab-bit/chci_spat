"""Unit tests for Bronze HR ingestion (`employees`, `timesheets`)."""

from __future__ import annotations

import shutil
from datetime import date
from pathlib import Path

from pyspark.sql import SparkSession

from spark_jobs.bronze.ingest_hr import TIMESHEETS_RAW_SCHEMA, run

EMP_HEADER = (
    "emp_id,full_name,department,role,seniority_level,hourly_rate_eur,hire_date,"
    "last_promotion_date,work_mode,distance_km,manager_id,is_active,termination_date,"
    "churn_type,snapshot_date"
)


def _write_employees(landing: Path, day: str, rows: list[str]) -> None:
    d = landing / "employees" / f"date={day}"
    d.mkdir(parents=True, exist_ok=True)
    (d / "part-000.csv").write_text("\n".join([EMP_HEADER, *rows]) + "\n", encoding="utf-8")


def _write_timesheets(spark: SparkSession, landing: Path, day: str, rows: list[tuple]) -> None:
    """Write `rows` as a single `part-000.parquet` file, like the landing zone."""
    d = landing / "timesheets" / f"date={day}"
    tmp = landing / "_tmp_parquet"
    spark.createDataFrame(rows, TIMESHEETS_RAW_SCHEMA).coalesce(1).write.mode("overwrite").parquet(
        str(tmp)
    )
    d.mkdir(parents=True, exist_ok=True)
    shutil.move(str(next(tmp.glob("part-*.parquet"))), d / "part-000.parquet")
    shutil.rmtree(tmp)


def _seed(spark: SparkSession, landing: Path, day: str) -> None:
    _write_employees(
        landing,
        day,
        [
            f"EMP_0001,Ann A,Data,Engineer,Senior,63.69,2023-01-31,2023-11-09,Hybrid,12.6,"
            f"EMP_0002,true,,,{day}",
            f"EMP_0002,Bob B,Sales,Manager,Lead,80.0,2020-05-01,2024-01-04,Remote,3.7,,false,"
            f"{day},voluntary,{day}",
        ],
    )
    wd = date.fromisoformat(day)
    _write_timesheets(
        spark,
        landing,
        day,
        [
            ("TS-1", "EMP_0001", "PRJ_0001", wd, 5.5),
            ("TS-1", "EMP_0001", "PRJ_0001", wd, 5.75),  # near-duplicate kept in Bronze
            ("TS-2", "EMP_0001", None, wd, -1.0),  # NULL project + bad hours kept
        ],
    )


def test_ingest_hr_raw_values_and_metadata(spark: SparkSession, tmp_path: Path) -> None:
    landing, lake = tmp_path / "landing", tmp_path / "lake"
    _seed(spark, landing, "2026-01-05")

    counts = run(spark, "2026-01-05", str(landing), str(lake))
    assert counts == {"employees": 2, "timesheets": 3}

    emp = spark.read.format("delta").load(str(lake / "bronze" / "employees"))
    bob = emp.filter("emp_id = 'EMP_0002'").first()
    assert bob["is_active"] is False
    assert bob["termination_date"] == "2026-01-05"  # raw string, not parsed
    assert bob["manager_id"] is None
    assert str(bob["_batch_date"]) == "2026-01-05"
    assert bob["_source_file"].endswith("part-000.csv")
    assert bob["_ingested_at"] is not None

    ts = spark.read.format("delta").load(str(lake / "bronze" / "timesheets"))
    assert ts.filter("ts_id = 'TS-1'").count() == 2
    assert ts.filter("project_id IS NULL AND hours < 0").count() == 1


def test_ingest_hr_is_idempotent(spark: SparkSession, tmp_path: Path) -> None:
    landing, lake = tmp_path / "landing", tmp_path / "lake"
    _seed(spark, landing, "2026-01-05")
    _seed(spark, landing, "2026-01-06")

    run(spark, "2026-01-05", str(landing), str(lake))
    run(spark, "2026-01-06", str(landing), str(lake))
    run(spark, "2026-01-05", str(landing), str(lake))

    emp = spark.read.format("delta").load(str(lake / "bronze" / "employees"))
    ts = spark.read.format("delta").load(str(lake / "bronze" / "timesheets"))
    assert emp.count() == 4
    assert ts.count() == 6
    assert ts.filter("_batch_date = '2026-01-06'").count() == 3


def test_ingest_hr_skips_missing_day(spark: SparkSession, tmp_path: Path) -> None:
    landing, lake = tmp_path / "landing", tmp_path / "lake"
    _seed(spark, landing, "2026-01-05")
    run(spark, "2026-01-05", str(landing), str(lake))

    counts = run(spark, "2026-01-07", str(landing), str(lake))
    assert counts == {"employees": None, "timesheets": None}
    emp = spark.read.format("delta").load(str(lake / "bronze" / "employees"))
    assert emp.count() == 2
