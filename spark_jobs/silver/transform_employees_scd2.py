"""Silver SCD Type 2 job: `bronze/employees` daily snapshots -> `silver/silver_employees_scd2`.

Design (documented in `docs/architecture.md`):
- Input: every daily snapshot in `bronze/employees` with `_batch_date <= batch_date`.
- A new version starts whenever any tracked column differs from the employee's previous
  snapshot. Tracked: `department`, `role`, `hourly_rate_eur`, `is_active`,
  `termination_date`, `churn_type`. (In the data, no other attribute ever changes.)
- Half-open intervals `[valid_from, valid_to)`: `valid_to` is the next version's
  `valid_from`; the open version has `valid_to = 9999-12-31` and `is_current = true`.
  Point-in-time join: `work_date >= valid_from AND work_date < valid_to`.
- The table is rebuilt from Bronze history on every run and overwritten. Re-running a
  `batch_date` therefore gives the identical table, and a backfill run in date order
  reproduces the state as of each day.
- Hard DQ checks (ERROR): non-null keys, unique `(emp_id, valid_from)`, contiguous
  non-overlapping intervals, exactly one current version per employee, every employee
  in the latest snapshot present.

Reference: `docs/STUDENT_ASSIGNMENT.md` (Sections 3.2, 4.2.B.1, 4.3) and `docs/SANITY_CHECKS.md`.
"""

from __future__ import annotations

import argparse
import uuid

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from dq.framework import DQCheckResult, enforce_dq_gate, write_audit_results
from spark_jobs.common.spark_session import get_lakehouse_root, get_spark_session, join_path
from spark_jobs.silver.hr_dq import make_result

TABLE_NAME = "silver_employees_scd2"
TRACKED_COLUMNS = (
    "department",
    "role",
    "hourly_rate_eur",
    "is_active",
    "termination_date",
    "churn_type",
)
OPEN_END_DATE = "9999-12-31"

OUTPUT_COLUMNS = (
    "emp_id",
    "full_name",
    "department",
    "role",
    "seniority_level",
    "hourly_rate_eur",
    "hire_date",
    "last_promotion_date",
    "work_mode",
    "distance_km",
    "manager_id",
    "is_active",
    "termination_date",
    "churn_type",
    "valid_from",
    "valid_to",
    "is_current",
    "version_number",
    "last_seen_date",
    "record_hash",
)


def _clean_str(column_name: str) -> Column:
    trimmed = F.trim(F.col(column_name))
    return F.when(F.length(trimmed) == 0, F.lit(None).cast("string")).otherwise(trimmed)


def _typed_snapshots(bronze_df: DataFrame, batch_date: str) -> DataFrame:
    """Cast raw Bronze strings to Silver types; one row per (emp_id, snapshot_date)."""
    snaps = (
        bronze_df.filter(F.col("_batch_date") <= F.to_date(F.lit(batch_date)))
        .select(
            _clean_str("emp_id").alias("emp_id"),
            _clean_str("full_name").alias("full_name"),
            _clean_str("department").alias("department"),
            _clean_str("role").alias("role"),
            _clean_str("seniority_level").alias("seniority_level"),
            F.col("hourly_rate_eur").cast("double").alias("hourly_rate_eur"),
            F.to_date(_clean_str("hire_date")).alias("hire_date"),
            F.to_date(_clean_str("last_promotion_date")).alias("last_promotion_date"),
            _clean_str("work_mode").alias("work_mode"),
            F.col("distance_km").cast("double").alias("distance_km"),
            _clean_str("manager_id").alias("manager_id"),
            F.col("is_active").cast("boolean").alias("is_active"),
            F.to_date(_clean_str("termination_date")).alias("termination_date"),
            _clean_str("churn_type").alias("churn_type"),
            F.coalesce(F.to_date(_clean_str("snapshot_date")), F.col("_batch_date")).alias(
                "snapshot_date"
            ),
            F.col("_ingested_at"),
        )
        .filter(F.col("emp_id").isNotNull())
    )
    latest_load = Window.partitionBy("emp_id", "snapshot_date").orderBy(
        F.col("_ingested_at").desc()
    )
    return (
        snaps.withColumn("_rn", F.row_number().over(latest_load))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "_ingested_at")
    )


def build_scd2(bronze_df: DataFrame, batch_date: str) -> DataFrame:
    """Collapse daily snapshots into SCD2 versions with half-open validity intervals."""
    snaps = _typed_snapshots(bronze_df, batch_date)
    record_hash = F.sha2(
        F.concat_ws(
            "||", *[F.coalesce(F.col(c).cast("string"), F.lit("<NULL>")) for c in TRACKED_COLUMNS]
        ),
        256,
    )
    by_day = Window.partitionBy("emp_id").orderBy("snapshot_date")
    flagged = (
        snaps.withColumn("record_hash", record_hash)
        .withColumn("_prev_hash", F.lag("record_hash").over(by_day))
        .withColumn(
            "_is_change",
            (F.col("_prev_hash").isNull() | (F.col("_prev_hash") != F.col("record_hash"))).cast(
                "int"
            ),
        )
        .withColumn(
            "version_number",
            F.sum("_is_change").over(by_day.rowsBetween(Window.unboundedPreceding, 0)),
        )
    )
    last_seen = flagged.groupBy("emp_id", "version_number").agg(
        F.max("snapshot_date").alias("last_seen_date")
    )
    by_version = Window.partitionBy("emp_id").orderBy("valid_from")
    next_from = F.lead("valid_from").over(by_version)
    return (
        flagged.filter(F.col("_is_change") == 1)
        .withColumnRenamed("snapshot_date", "valid_from")
        .join(last_seen, ["emp_id", "version_number"])
        .withColumn("valid_to", F.coalesce(next_from, F.to_date(F.lit(OPEN_END_DATE))))
        .withColumn("is_current", next_from.isNull())
        .select(*OUTPUT_COLUMNS)
    )


def scd2_checks(
    scd2_df: DataFrame, bronze_df: DataFrame, batch_date: str, run_id: str
) -> list[DQCheckResult]:
    """Hard (ERROR) integrity checks for the SCD2 table."""
    common = {"run_id": run_id, "batch_date": batch_date, "table_name": TABLE_NAME}
    total = scd2_df.count()
    employees = scd2_df.select("emp_id").distinct().count()

    duplicate_keys = total - scd2_df.select("emp_id", "valid_from").distinct().count()

    by_version = Window.partitionBy("emp_id").orderBy("valid_from")
    with_next = scd2_df.withColumn("_next_from", F.lead("valid_from").over(by_version))
    broken_intervals = with_next.filter(
        (F.col("valid_from") >= F.col("valid_to"))
        | (F.col("_next_from").isNotNull() & (F.col("valid_to") != F.col("_next_from")))
    ).count()

    bad_current = (
        scd2_df.groupBy("emp_id")
        .agg(F.sum(F.col("is_current").cast("int")).alias("n"))
        .filter(F.col("n") != 1)
        .count()
    )

    latest_date = (
        bronze_df.filter(F.col("_batch_date") <= F.to_date(F.lit(batch_date)))
        .agg(F.max("_batch_date"))
        .first()[0]
    )
    latest_ids = (
        bronze_df.filter(F.col("_batch_date") == F.lit(latest_date))
        .select(F.trim("emp_id").alias("emp_id"))
        .filter(F.length("emp_id") > 0)
        .distinct()
    )
    missing = latest_ids.join(scd2_df.select("emp_id").distinct(), "emp_id", "left_anti").count()

    return [
        make_result(
            **common,
            check_name="not_null_emp_id",
            severity="ERROR",
            failed_count=scd2_df.filter(F.col("emp_id").isNull()).count(),
            total_count=total,
        ),
        make_result(
            **common,
            check_name="not_null_valid_from",
            severity="ERROR",
            failed_count=scd2_df.filter(F.col("valid_from").isNull()).count(),
            total_count=total,
        ),
        make_result(
            **common,
            check_name="unique_emp_id_valid_from",
            severity="ERROR",
            failed_count=duplicate_keys,
            total_count=total,
        ),
        make_result(
            **common,
            check_name="scd2_no_overlap_no_gap",
            severity="ERROR",
            failed_count=broken_intervals,
            total_count=total,
        ),
        make_result(
            **common,
            check_name="scd2_one_current_per_emp",
            severity="ERROR",
            failed_count=bad_current,
            total_count=employees,
        ),
        make_result(
            **common,
            check_name="scd2_covers_latest_snapshot",
            severity="ERROR",
            failed_count=missing,
            total_count=latest_ids.count(),
        ),
    ]


def run(
    spark: SparkSession,
    batch_date: str,
    lakehouse_root: str | None = None,
    run_id: str | None = None,
) -> DataFrame:
    """Rebuild `silver/silver_employees_scd2` as of `batch_date` and run its DQ gate."""
    lakehouse = get_lakehouse_root(lakehouse_root)
    resolved_run_id = run_id or f"manual-{batch_date}-{uuid.uuid4().hex[:8]}"
    bronze_path = join_path(lakehouse, "bronze", "employees")
    silver_path = join_path(lakehouse, "silver", TABLE_NAME)
    audit_path = join_path(lakehouse, "dq", "dq_audit_results")

    bronze_df = spark.read.format("delta").load(bronze_path)
    scd2_df = (
        build_scd2(bronze_df, batch_date)
        .withColumn("_batch_date", F.to_date(F.lit(batch_date)))
        .withColumn("_run_id", F.lit(resolved_run_id))
        .withColumn("_silver_updated_at", F.current_timestamp())
    )
    (
        scd2_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(silver_path)
    )

    written = spark.read.format("delta").load(silver_path)
    results = scd2_checks(written, bronze_df, batch_date, resolved_run_id)
    write_audit_results(spark, results, audit_path)
    enforce_dq_gate(results)
    return written


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build silver_employees_scd2.")
    parser.add_argument("--batch-date", required=True, help="Logical batch date (YYYY-MM-DD).")
    parser.add_argument("--lakehouse-root", default=None, help="Override Lakehouse root path.")
    parser.add_argument("--run-id", default=None, help="Orchestrator run identifier.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = get_spark_session("silver-transform-employees-scd2")
    try:
        df = run(spark, args.batch_date, args.lakehouse_root, args.run_id)
        current = df.filter("is_current").count()
        print(
            f"Silver employees SCD2 batch_date={args.batch_date}: "
            f"versions={df.count()}, current={current}"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
