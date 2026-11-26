"""Bronze ingestion for the Finance domain (`fx_rates`, `invoices`, `expenses`).

- Explicit raw schemas, zero lossy transformations: date columns stay as raw strings
  (`invoices.issued_at`/`paid_at` mix ISO and `DD.MM.YYYY`; parsing is a Silver concern),
  duplicates and negative amounts (credit notes) are kept.
- `fx_rates` is delivered on weekdays only; a missing partition is skipped, not an error.
- Idempotent: only the target `_batch_date` partition is replaced.
"""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from spark_jobs.common.bronze_io import add_metadata, path_exists, write_daily
from spark_jobs.common.spark_session import (
    get_lakehouse_root,
    get_landing_root,
    get_spark_session,
    join_path,
)

FX_RATES_RAW_SCHEMA = StructType(
    [
        StructField("rate_date", StringType(), True),
        StructField("currency_from", StringType(), True),
        StructField("currency_to", StringType(), True),
        StructField("rate", DoubleType(), True),
    ]
)

INVOICES_RAW_SCHEMA = StructType(
    [
        StructField("inv_id", StringType(), True),
        StructField("client_id", StringType(), True),
        StructField("project_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("issued_at", StringType(), True),
        StructField("paid_at", StringType(), True),
    ]
)

EXPENSES_RAW_SCHEMA = StructType(
    [
        StructField("exp_id", StringType(), True),
        StructField("department", StringType(), True),
        StructField("category", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("expense_date", StringType(), True),
    ]
)


def _read_fx_rates(spark: SparkSession, path: str) -> DataFrame:
    # Each file is a pretty-printed JSON array, so it needs multiLine.
    return spark.read.schema(FX_RATES_RAW_SCHEMA).option("multiLine", "true").json(path)


def _read_invoices(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.schema(INVOICES_RAW_SCHEMA).json(path)


def _read_expenses(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.option("header", "true").schema(EXPENSES_RAW_SCHEMA).csv(path)


# table -> (landing file name, reader)
DATASETS = {
    "fx_rates": ("part-000.json", _read_fx_rates),
    "invoices": ("part-000.jsonl", _read_invoices),
    "expenses": ("part-000.csv", _read_expenses),
}


def run(
    spark: SparkSession,
    batch_date: str,
    landing_root: str | None = None,
    lakehouse_root: str | None = None,
) -> dict[str, int | None]:
    """Ingest finance datasets for `batch_date`; returns rows written (None if skipped)."""
    resolved_landing = get_landing_root(landing_root)
    resolved_lakehouse = get_lakehouse_root(lakehouse_root)

    written: dict[str, int | None] = {}
    for table, (file_name, reader) in DATASETS.items():
        source = join_path(resolved_landing, table, f"date={batch_date}", file_name)
        if not path_exists(spark, source):
            print(f"[bronze/{table}] no file for {batch_date} ({source}); skipping")
            written[table] = None
            continue

        df = add_metadata(reader(spark, source), batch_date).cache()
        write_daily(df, join_path(resolved_lakehouse, "bronze", table), batch_date)
        written[table] = df.count()
        df.unpersist()
        print(f"[bronze/{table}] wrote {written[table]} rows for {batch_date}")
    return written


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest Finance datasets into Bronze Delta.")
    parser.add_argument("--batch-date", required=True, help="Logical batch date (YYYY-MM-DD).")
    parser.add_argument("--landing-root", default=None, help="Override Landing Zone path.")
    parser.add_argument("--lakehouse-root", default=None, help="Override Lakehouse root path.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    spark = get_spark_session("bronze-ingest-finance")
    try:
        run(spark, args.batch_date, args.landing_root, args.lakehouse_root)
    finally:
        spark.stop()
