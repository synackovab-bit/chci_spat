"""Unit tests for HR Silver jobs: `silver_employees_scd2` and `silver_timesheets`."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from dq.framework import DQGateFailedError
from spark_jobs.bronze.ingest_hr import EMPLOYEES_RAW_SCHEMA, TIMESHEETS_RAW_SCHEMA
from spark_jobs.common.bronze_io import add_metadata, write_daily
from spark_jobs.silver import transform_employees_scd2, transform_timesheets
from spark_jobs.silver.hr_dq import make_result

D1, D2, D3 = "2026-01-01", "2026-01-02", "2026-01-03"


def _emp(emp_id: str, rate: float, active: bool, term: str | None, churn: str | None, day: str):
    return (
        emp_id,
        f"Name {emp_id}",
        "Data",
        "Engineer",
        "Mid",
        rate,
        "2023-01-01",
        "2024-01-01",
        "Hybrid",
        5.0,
        None,
        active,
        term,
        churn,
        day,
    )


def _seed_employees(spark: SparkSession, lake: Path) -> None:
    days = {
        D1: [
            _emp("EMP_1", 50.0, True, None, None, D1),
            _emp("EMP_2", 40.0, True, None, None, D1),
            _emp("EMP_3", 30.0, True, None, None, D1),
        ],
        D2: [
            _emp("EMP_1", 50.0, True, None, None, D2),
            _emp("EMP_2", 40.0, False, D2, "voluntary", D2),  # leaves on D2
            _emp("EMP_3", 30.0, True, None, None, D2),
        ],
        D3: [
            _emp("EMP_1", 60.0, True, None, None, D3),  # raise on D3
            _emp("EMP_2", 40.0, False, D2, "voluntary", D3),
            _emp("EMP_3", 30.0, True, None, None, D3),
        ],
    }
    for day, rows in days.items():
        df = add_metadata(spark.createDataFrame(rows, EMPLOYEES_RAW_SCHEMA), day)
        write_daily(df, str(lake / "bronze" / "employees"), day)


def _seed_timesheets(spark: SparkSession, lake: Path) -> None:
    wd = date.fromisoformat(D3)
    rows = [
        ("TS-1", "EMP_1", "PRJ_1", wd, 2.0),
        ("TS-1", "EMP_1", "PRJ_1", wd, 2.5),  # near-duplicate, higher hours -> dropped
        ("TS-2", "EMP_1", None, wd, 3.0),  # missing project
        ("TS-3", "EMP_3", "PRJ_1", wd, -1.0),  # non-positive hours
        ("TS-4", "EMP_3", "PRJ_1", wd, 25.0),  # over 24
        ("TS-5", "EMP_2", "PRJ_1", wd, 4.0),  # after termination (D2)
        ("TS-6", "EMP_9", "PRJ_1", wd, 4.0),  # unknown employee
        ("TS-7", "EMP_3", "PRJ_2", wd, 8.0),  # valid
    ]
    df = add_metadata(spark.createDataFrame(rows, TIMESHEETS_RAW_SCHEMA), D3)
    write_daily(df, str(lake / "bronze" / "timesheets"), D3)


def _versions(df, emp_id: str) -> list[tuple]:
    return [
        (str(r["valid_from"]), str(r["valid_to"]), r["is_current"], r["hourly_rate_eur"])
        for r in df.filter(F.col("emp_id") == emp_id).orderBy("valid_from").collect()
    ]


def test_scd2_versions_and_half_open_intervals(spark: SparkSession, tmp_path: Path) -> None:
    lake = tmp_path / "lake"
    _seed_employees(spark, lake)

    df = transform_employees_scd2.run(spark, D3, str(lake), run_id="t1")

    assert df.count() == 5
    assert _versions(df, "EMP_1") == [
        (D1, D3, False, 50.0),
        (D3, "9999-12-31", True, 60.0),
    ]
    emp2 = df.filter("emp_id = 'EMP_2'").orderBy("valid_from").collect()
    assert [str(r["valid_from"]) for r in emp2] == [D1, D2]
    assert emp2[1]["is_active"] is False
    assert str(emp2[1]["termination_date"]) == D2
    assert emp2[1]["churn_type"] == "voluntary"
    assert _versions(df, "EMP_3") == [(D1, "9999-12-31", True, 30.0)]

    audit = spark.read.format("delta").load(str(lake / "dq" / "dq_audit_results"))
    scd2_audit = audit.filter("table_name = 'silver_employees_scd2'")
    assert scd2_audit.count() == 6
    assert scd2_audit.filter("status != 'PASS'").count() == 0


def test_scd2_is_as_of_batch_date_and_idempotent(spark: SparkSession, tmp_path: Path) -> None:
    lake = tmp_path / "lake"
    _seed_employees(spark, lake)

    as_of_d2 = transform_employees_scd2.run(spark, D2, str(lake), run_id="t2")
    assert _versions(as_of_d2, "EMP_1") == [(D1, "9999-12-31", True, 50.0)]  # raise not yet

    first = transform_employees_scd2.run(spark, D3, str(lake), run_id="t3")
    rows_first = sorted(first.drop("_silver_updated_at", "_run_id").collect())
    second = transform_employees_scd2.run(spark, D3, str(lake), run_id="t4")
    rows_second = sorted(second.drop("_silver_updated_at", "_run_id").collect())
    assert rows_first == rows_second


def test_scd2_checks_catch_overlap(spark: SparkSession, tmp_path: Path) -> None:
    lake = tmp_path / "lake"
    _seed_employees(spark, lake)
    bronze = spark.read.format("delta").load(str(lake / "bronze" / "employees"))
    good = transform_employees_scd2.build_scd2(bronze, D3)
    # Stretch EMP_1's first version past the start of the next one -> overlap.
    broken = good.withColumn(
        "valid_to",
        F.when(
            (F.col("emp_id") == "EMP_1") & ~F.col("is_current"), F.to_date(F.lit("2026-01-05"))
        ).otherwise(F.col("valid_to")),
    )
    results = {
        r.check_name: r for r in transform_employees_scd2.scd2_checks(broken, bronze, D3, "t5")
    }
    assert results["scd2_no_overlap_no_gap"].status == "FAIL"
    assert results["unique_emp_id_valid_from"].status == "PASS"


def test_timesheets_dedup_and_quarantine_reasons(spark: SparkSession, tmp_path: Path) -> None:
    lake = tmp_path / "lake"
    _seed_employees(spark, lake)
    _seed_timesheets(spark, lake)
    transform_employees_scd2.run(spark, D3, str(lake), run_id="t6")

    transform_timesheets.run(spark, D3, str(lake), run_id="t6")
    transform_timesheets.run(spark, D3, str(lake), run_id="t7")  # re-run: no duplicates

    valid = spark.read.format("delta").load(str(lake / "silver" / "silver_timesheets"))
    quarantine = spark.read.format("delta").load(str(lake / "silver_quarantine" / "timesheets"))

    assert sorted(r["ts_id"] for r in valid.collect()) == ["TS-1", "TS-7"]
    ts1 = valid.filter("ts_id = 'TS-1'").first()
    assert ts1["hours"] == 2.0
    assert ts1["_duplicate_count"] == 2

    reasons = {r["ts_id"]: r["dq_reasons"] for r in quarantine.collect()}
    assert reasons == {
        "TS-2": ["missing_project_id"],
        "TS-3": ["hours_non_positive"],
        "TS-4": ["hours_over_24"],
        "TS-5": ["work_after_termination"],
        "TS-6": ["unknown_employee"],
    }

    audit = spark.read.format("delta").load(str(lake / "dq" / "dq_audit_results"))
    status = {
        r["check_name"]: r["status"]
        for r in audit.filter("table_name = 'silver_timesheets'").collect()
    }
    assert status["row_reconciliation"] == "PASS"
    assert status["unique_ts_id"] == "PASS"
    assert status["quarantine_ratio"] == "WARN"  # 5 of 7 rows quarantined
    assert status["quarantine_work_after_termination"] == "WARN"


def test_hard_failure_raises_gate(spark: SparkSession) -> None:
    from dq.framework import enforce_dq_gate

    bad = make_result(
        run_id="t8",
        batch_date=D1,
        table_name="silver_timesheets",
        check_name="unique_ts_id",
        severity="ERROR",
        failed_count=1,
        total_count=10,
    )
    with pytest.raises(DQGateFailedError):
        enforce_dq_gate([bad])
