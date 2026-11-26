"""Shared helpers for Bronze ingestion jobs.

Usage pattern for a daily table::

    src = join_path(landing_root, "invoices", f"date={batch_date}", "part-000.jsonl")
    if not path_exists(spark, src):
        print(f"No file for {batch_date}, skipping")
        return
    df = add_metadata(spark.read.schema(SCHEMA).json(src), batch_date)
    write_daily(df, join_path(lakehouse_root, "bronze", "invoices"), batch_date)
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def add_metadata(df: DataFrame, batch_date: str) -> DataFrame:
    """Append the standard Bronze technical columns.

    Call this directly after the read: `input_file_name()` is only reliable
    before shuffles/joins.
    """
    return (
        df.withColumn("_batch_date", F.to_date(F.lit(batch_date)))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )


def path_exists(spark: SparkSession, path: str) -> bool:
    """Return True if a local or `s3a://` path (or glob) matches at least one file."""
    jvm = spark._jvm
    hadoop_path = jvm.org.apache.hadoop.fs.Path(path)
    fs = hadoop_path.getFileSystem(spark._jsc.hadoopConfiguration())
    matches = fs.globStatus(hadoop_path)  # None (plain path) or empty (glob) when missing
    return matches is not None and len(matches) > 0


def write_daily(df: DataFrame, target_path: str, batch_date: str) -> None:
    """Idempotently replace only `batch_date` in a Delta table partitioned by `_batch_date`.

    Re-running the same date overwrites that partition; other dates are untouched.
    `replaceWhere` rejects rows whose `_batch_date` differs from `batch_date`.
    Callers should skip the call when the source file is absent, since an empty
    frame would delete that day's existing rows.
    """
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"_batch_date = '{batch_date}'")
        .partitionBy("_batch_date")
        .save(target_path)
    )
