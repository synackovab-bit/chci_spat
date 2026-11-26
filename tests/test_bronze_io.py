"""Unit tests for shared Bronze helpers."""

from __future__ import annotations

from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from spark_jobs.common.bronze_io import add_metadata, path_exists, write_daily


def _read_day(spark: SparkSession, csv_path: Path, batch_date: str):
    df = spark.read.option("header", "true").csv(str(csv_path))
    return add_metadata(df, batch_date)


@pytest.fixture
def csv_file(tmp_path: Path) -> Path:
    path = tmp_path / "in.csv"
    path.write_text("id,val\n1,a\n2,b\n3,c\n")
    return path


def test_add_metadata_columns(spark: SparkSession, csv_file: Path) -> None:
    df = _read_day(spark, csv_file, "2026-01-05")
    row = df.first()
    assert {"_batch_date", "_ingested_at", "_source_file"} <= set(df.columns)
    assert str(row["_batch_date"]) == "2026-01-05"
    assert row["_ingested_at"] is not None
    assert row["_source_file"].endswith("in.csv")


def test_write_daily_is_idempotent(spark: SparkSession, csv_file: Path, tmp_path: Path) -> None:
    target = str(tmp_path / "bronze" / "t")
    write_daily(_read_day(spark, csv_file, "2026-01-05"), target, "2026-01-05")
    write_daily(_read_day(spark, csv_file, "2026-01-05"), target, "2026-01-05")
    assert spark.read.format("delta").load(target).count() == 3


def test_write_daily_preserves_other_days(
    spark: SparkSession, csv_file: Path, tmp_path: Path
) -> None:
    target = str(tmp_path / "bronze" / "t")
    write_daily(_read_day(spark, csv_file, "2026-01-05"), target, "2026-01-05")
    write_daily(_read_day(spark, csv_file, "2026-01-06"), target, "2026-01-06")
    write_daily(_read_day(spark, csv_file, "2026-01-05"), target, "2026-01-05")
    df = spark.read.format("delta").load(target)
    assert df.count() == 6
    assert df.filter("_batch_date = '2026-01-06'").count() == 3


def test_write_daily_rejects_mismatched_batch_date(
    spark: SparkSession, csv_file: Path, tmp_path: Path
) -> None:
    target = str(tmp_path / "bronze" / "t")
    write_daily(_read_day(spark, csv_file, "2026-01-05"), target, "2026-01-05")
    with pytest.raises(Exception):  # noqa: B017 - Delta raises an AnalysisException subclass
        write_daily(_read_day(spark, csv_file, "2026-01-06"), target, "2026-01-05")


def test_path_exists(spark: SparkSession, csv_file: Path, tmp_path: Path) -> None:
    assert path_exists(spark, str(csv_file))
    assert path_exists(spark, str(tmp_path / "*.csv"))
    assert not path_exists(spark, str(tmp_path / "missing.csv"))
    assert not path_exists(spark, str(tmp_path / "missing" / "*.csv"))
