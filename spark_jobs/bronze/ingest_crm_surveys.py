"""Bronze ingestion for `crm_events` and `employee_surveys` (daily JSONL batches).

- `<landing_root>/crm_events/date=<batch_date>/part-000.jsonl` (every day)
- `<landing_root>/employee_surveys/date=<batch_date>/part-000.jsonl` (survey wave days only)

Raw values are preserved:
- `crm_events.payload` is read as `StringType`, so the polymorphic nested JSON is stored as
  its original JSON text (no event-type fields dropped); `timestamp` stays a raw string.
- `employee_surveys.answers` keeps the nested `array<struct<...>>`; duplicates, out-of-range
  scores (-1, 11) and NULL `written_answer` are kept for Silver to handle.
Reads use FAILFAST so a malformed record fails the run instead of silently becoming NULL.
A missing source file (non-wave survey days) is skipped, never written as an empty frame.

Reference: `docs/STUDENT_ASSIGNMENT.md` (Sections 3.1, 3.3, 4.2.A) and `docs/SANITY_CHECKS.md`.
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
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

CRM_EVENTS_RAW_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), nullable=True),
        StructField("client_id", StringType(), nullable=True),
        StructField("event_type", StringType(), nullable=True),
        StructField("timestamp", StringType(), nullable=True),
        StructField("payload", StringType(), nullable=True),  # raw JSON text
    ]
)

SURVEY_ANSWER_SCHEMA = StructType(
    [
        StructField("question_id", StringType(), nullable=True),
        StructField("dimension", StringType(), nullable=True),
        StructField("numeric_score", IntegerType(), nullable=True),
        StructField("written_answer", StringType(), nullable=True),
    ]
)

EMPLOYEE_SURVEYS_RAW_SCHEMA = StructType(
    [
        StructField("survey_id", StringType(), nullable=True),
        StructField("emp_id", StringType(), nullable=True),
        StructField("submitted_at", StringType(), nullable=True),
        StructField("enps_score", IntegerType(), nullable=True),
        StructField("overall_comment", StringType(), nullable=True),
        StructField("answers", ArrayType(SURVEY_ANSWER_SCHEMA), nullable=True),
    ]
)


def _ingest_jsonl(
    spark: SparkSession,
    table: str,
    schema: StructType,
    batch_date: str,
    landing_root: str,
    lakehouse_root: str,
) -> int | None:
    """Ingest one day of a JSONL dataset. Returns the row count, or None if no file exists."""
    src = join_path(landing_root, table, f"date={batch_date}", "part-000.jsonl")
    if not path_exists(spark, src):
        print(f"[{table}] no file for {batch_date}, skipping")
        return None
    raw = spark.read.option("mode", "FAILFAST").schema(schema).json(src)
    df = add_metadata(raw, batch_date)
    write_daily(df, join_path(lakehouse_root, "bronze", table), batch_date)
    return df.count()


def run(
    spark: SparkSession,
    batch_date: str,
    landing_root: str | None = None,
    lakehouse_root: str | None = None,
) -> dict[str, int | None]:
    """Ingest `crm_events` and `employee_surveys` for `batch_date` into Bronze Delta tables."""
    landing = get_landing_root(landing_root)
    lakehouse = get_lakehouse_root(lakehouse_root)
    return {
        "crm_events": _ingest_jsonl(
            spark, "crm_events", CRM_EVENTS_RAW_SCHEMA, batch_date, landing, lakehouse
        ),
        "employee_surveys": _ingest_jsonl(
            spark, "employee_surveys", EMPLOYEE_SURVEYS_RAW_SCHEMA, batch_date, landing, lakehouse
        ),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest CRM & Survey datasets into Bronze Delta.")
    parser.add_argument("--batch-date", required=True, help="Logical batch date (YYYY-MM-DD).")
    parser.add_argument("--landing-root", default=None, help="Override Landing Zone path.")
    parser.add_argument("--lakehouse-root", default=None, help="Override Lakehouse root path.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = get_spark_session("bronze-ingest-crm-surveys")
    try:
        counts = run(spark, args.batch_date, args.landing_root, args.lakehouse_root)
        print(f"Bronze CRM/surveys batch_date={args.batch_date}: {counts}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
