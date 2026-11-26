"""Bronze ingestion for the HR domain: `employees` (CSV) and `timesheets` (Parquet).

Both are daily Hive-partitioned drops:
- `<landing_root>/employees/date=<batch_date>/part-000.csv`   (full daily snapshot, 1,200 rows)
- `<landing_root>/timesheets/date=<batch_date>/part-000.parquet`

Raw values are preserved: duplicates, NULL `project_id`, and out-of-range `hours` are kept
(cleaning happens in Silver). Employee date columns stay as raw strings. The employees CSV is
read in FAILFAST mode so a malformed numeric/boolean value fails the run instead of being
silently turned into NULL. A missing source file is skipped, never written as an empty frame
(that would wipe the day's existing partition).

Reference: `docs/STUDENT_ASSIGNMENT.md` (Sections 3.2, 4.2.A) and `docs/SANITY_CHECKS.md`.
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)

from spark_jobs.common.bronze_io import add_metadata, path_exists, write_daily
from spark_jobs.common.spark_session import (
    get_lakehouse_root,
    get_landing_root,
    get_spark_session,
    join_path,
)

EMPLOYEES_RAW_SCHEMA = StructType(
    [
        StructField("emp_id", StringType(), nullable=True),
        StructField("full_name", StringType(), nullable=True),
        StructField("department", StringType(), nullable=True),
        StructField("role", StringType(), nullable=True),
        StructField("seniority_level", StringType(), nullable=True),
        StructField("hourly_rate_eur", DoubleType(), nullable=True),
        StructField("hire_date", StringType(), nullable=True),
        StructField("last_promotion_date", StringType(), nullable=True),
        StructField("work_mode", StringType(), nullable=True),
        StructField("distance_km", DoubleType(), nullable=True),
        StructField("manager_id", StringType(), nullable=True),
        StructField("is_active", BooleanType(), nullable=True),
        StructField("termination_date", StringType(), nullable=True),
        StructField("churn_type", StringType(), nullable=True),
        StructField("snapshot_date", StringType(), nullable=True),
    ]
)

TIMESHEETS_RAW_SCHEMA = StructType(
    [
        StructField("ts_id", StringType(), nullable=True),
        StructField("emp_id", StringType(), nullable=True),
        StructField("project_id", StringType(), nullable=True),
        StructField("work_date", DateType(), nullable=True),
        StructField("hours", DoubleType(), nullable=True),
    ]
)


def ingest_employees(
    spark: SparkSession, batch_date: str, landing_root: str, lakehouse_root: str
) -> int | None:
    """Ingest one employees snapshot. Returns the row count, or None if no file exists."""
    src = join_path(landing_root, "employees", f"date={batch_date}", "part-000.csv")
    if not path_exists(spark, src):
        print(f"[employees] no file for {batch_date}, skipping")
        return None
    raw = (
        spark.read.option("header", "true")
        .option("mode", "FAILFAST")
        .schema(EMPLOYEES_RAW_SCHEMA)
        .csv(src)
    )
    df = add_metadata(raw, batch_date)
    write_daily(df, join_path(lakehouse_root, "bronze", "employees"), batch_date)
    return df.count()


def ingest_timesheets(
    spark: SparkSession, batch_date: str, landing_root: str, lakehouse_root: str
) -> int | None:
    """Ingest one day of timesheets. Returns the row count, or None if no file exists."""
    src = join_path(landing_root, "timesheets", f"date={batch_date}", "part-000.parquet")
    if not path_exists(spark, src):
        print(f"[timesheets] no file for {batch_date}, skipping")
        return None
    df = add_metadata(spark.read.schema(TIMESHEETS_RAW_SCHEMA).parquet(src), batch_date)
    write_daily(df, join_path(lakehouse_root, "bronze", "timesheets"), batch_date)
    return df.count()


def run(
    spark: SparkSession,
    batch_date: str,
    landing_root: str | None = None,
    lakehouse_root: str | None = None,
) -> dict[str, int | None]:
    """Ingest `employees` and `timesheets` for `batch_date` into Bronze Delta tables."""
    landing = get_landing_root(landing_root)
    lakehouse = get_lakehouse_root(lakehouse_root)
    return {
        "employees": ingest_employees(spark, batch_date, landing, lakehouse),
        "timesheets": ingest_timesheets(spark, batch_date, landing, lakehouse),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest HR datasets into Bronze Delta.")
    parser.add_argument("--batch-date", required=True, help="Logical batch date (YYYY-MM-DD).")
    parser.add_argument("--landing-root", default=None, help="Override Landing Zone path.")
    parser.add_argument("--lakehouse-root", default=None, help="Override Lakehouse root path.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = get_spark_session("bronze-ingest-hr")
    try:
        counts = run(spark, args.batch_date, args.landing_root, args.lakehouse_root)
        print(f"Bronze HR batch_date={args.batch_date}: {counts}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
