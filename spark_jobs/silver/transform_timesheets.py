"""Silver job: `bronze/timesheets` -> `silver/silver_timesheets` + `silver_quarantine/timesheets`.

Processes one `_batch_date` at a time and replaces only that partition (idempotent).

1. Deduplicate by `ts_id`. Tie-breaker (documented in `docs/architecture.md`): keep the row
   with the lowest `hours`, then lowest `project_id`, then the latest `_ingested_at`.
   Profiling showed duplicates never cross daily files, only `hours` differs between copies,
   and the original (first row in the file) always has the lowest `hours`, so this rule
   keeps the original deterministically. `_duplicate_count` records how many raw rows
   shared the `ts_id`.
2. Point-in-time join to `silver_employees_scd2` on
   `emp_id AND work_date >= valid_from AND work_date < valid_to`
   (the SCD2 job must run first for the same `batch_date`).
3. Route rows with any failed rule to quarantine with `dq_reasons: array<string>`:
   `missing_ts_id`, `missing_project_id`, `missing_hours`, `hours_non_positive`,
   `hours_over_24`, `unknown_employee`, `work_after_termination`.
4. Audit: ERROR checks (non-null and unique `ts_id`, row reconciliation: valid + quarantined
   must equal the distinct `ts_id` count in Bronze, so no row is silently lost) and
   WARN checks (quarantine ratio
   > 8%, duplicate rate > 10%, zombie timesheets, count per quarantine reason).

Reference: `docs/STUDENT_ASSIGNMENT.md` (Sections 3.2, 3.5, 4.2.B.2) and `docs/SANITY_CHECKS.md`.
"""

from __future__ import annotations

import argparse
import uuid

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from dq.framework import DQCheckResult, enforce_dq_gate, write_audit_results
from spark_jobs.common.bronze_io import write_daily
from spark_jobs.common.spark_session import get_lakehouse_root, get_spark_session, join_path
from spark_jobs.silver.hr_dq import make_result, ratio_result

TABLE_NAME = "silver_timesheets"
QUARANTINE_RATIO_MAX = 0.08
DUPLICATE_RATIO_MAX = 0.10
REASONS = (
    "missing_ts_id",
    "missing_project_id",
    "missing_hours",
    "hours_non_positive",
    "hours_over_24",
    "unknown_employee",
    "work_after_termination",
)
BASE_COLUMNS = (
    "ts_id",
    "emp_id",
    "project_id",
    "work_date",
    "hours",
    "_duplicate_count",
    "_batch_date",
    "_source_file",
    "_ingested_at",
)


def deduplicate(bronze_day: DataFrame) -> DataFrame:
    """One row per `ts_id` (rows with NULL `ts_id` are all kept, for quarantine)."""
    tie_break = Window.partitionBy("ts_id").orderBy(
        F.col("hours").asc_nulls_last(),
        F.col("project_id").asc_nulls_last(),
        F.col("_ingested_at").desc(),
    )
    ranked = bronze_day.withColumn("_rn", F.row_number().over(tie_break)).withColumn(
        "_duplicate_count", F.count(F.lit(1)).over(Window.partitionBy("ts_id"))
    )
    keep = F.col("ts_id").isNull() | (F.col("_rn") == 1)
    return (
        ranked.filter(keep)
        .withColumn(
            "_duplicate_count",
            F.when(F.col("ts_id").isNull(), F.lit(1)).otherwise(F.col("_duplicate_count")),
        )
        .drop("_rn")
    )


def evaluate(dedup_df: DataFrame, scd2_df: DataFrame) -> DataFrame:
    """Attach the point-in-time employee version and the list of failed rules."""
    versions = scd2_df.select(
        F.col("emp_id").alias("_e_emp_id"),
        F.col("valid_from").alias("_e_valid_from"),
        F.col("valid_to").alias("_e_valid_to"),
        F.col("termination_date").alias("_e_termination_date"),
    )
    joined = dedup_df.join(
        F.broadcast(versions),
        (F.col("emp_id") == F.col("_e_emp_id"))
        & (F.col("work_date") >= F.col("_e_valid_from"))
        & (F.col("work_date") < F.col("_e_valid_to")),
        "left",
    )
    reasons = F.array(
        F.when(F.col("ts_id").isNull(), F.lit("missing_ts_id")),
        F.when(F.col("project_id").isNull(), F.lit("missing_project_id")),
        F.when(F.col("hours").isNull(), F.lit("missing_hours")),
        F.when(F.col("hours") <= 0, F.lit("hours_non_positive")),
        F.when(F.col("hours") > 24, F.lit("hours_over_24")),
        F.when(F.col("_e_emp_id").isNull(), F.lit("unknown_employee")),
        F.when(
            F.col("_e_termination_date").isNotNull()
            & (F.col("work_date") >= F.col("_e_termination_date")),
            F.lit("work_after_termination"),
        ),
    )
    return joined.withColumn("dq_reasons", F.filter(reasons, lambda r: r.isNotNull())).select(
        *BASE_COLUMNS, "dq_reasons"
    )


def timesheet_checks(
    *,
    valid_df: DataFrame,
    quarantine_df: DataFrame,
    bronze_day: DataFrame,
    batch_date: str,
    run_id: str,
) -> list[DQCheckResult]:
    common = {"run_id": run_id, "batch_date": batch_date, "table_name": TABLE_NAME}
    bronze_count = bronze_day.count()
    # Rows that must survive dedup, counted independently from the Bronze input.
    expected_kept = (
        bronze_day.select("ts_id").distinct().filter(F.col("ts_id").isNotNull()).count()
        + bronze_day.filter(F.col("ts_id").isNull()).count()
    )
    valid_count = valid_df.count()
    quarantine_count = quarantine_df.count()
    kept = valid_count + quarantine_count
    duplicates_removed = bronze_count - kept

    reason_counts = {
        r["reason"]: r["n"]
        for r in quarantine_df.select(F.explode("dq_reasons").alias("reason"))
        .groupBy("reason")
        .agg(F.count(F.lit(1)).alias("n"))
        .collect()
    }

    results = [
        make_result(
            **common,
            check_name="not_null_ts_id",
            severity="ERROR",
            failed_count=valid_df.filter(F.col("ts_id").isNull()).count(),
            total_count=valid_count,
        ),
        make_result(
            **common,
            check_name="unique_ts_id",
            severity="ERROR",
            failed_count=valid_count - valid_df.select("ts_id").distinct().count(),
            total_count=valid_count,
        ),
        make_result(
            **common,
            check_name="row_reconciliation",
            severity="ERROR",
            failed_count=abs(kept - expected_kept),
            total_count=bronze_count,
        ),
        ratio_result(
            **common,
            check_name="quarantine_ratio",
            failed_count=quarantine_count,
            total_count=kept,
            max_ratio=QUARANTINE_RATIO_MAX,
        ),
        ratio_result(
            **common,
            check_name="duplicate_rate",
            failed_count=duplicates_removed,
            total_count=bronze_count,
            max_ratio=DUPLICATE_RATIO_MAX,
        ),
    ]
    results += [
        make_result(
            **common,
            check_name=f"quarantine_{reason}",
            severity="WARN",
            failed_count=reason_counts.get(reason, 0),
            total_count=kept,
        )
        for reason in REASONS
    ]
    return results


def run(
    spark: SparkSession,
    batch_date: str,
    lakehouse_root: str | None = None,
    run_id: str | None = None,
) -> tuple[DataFrame, DataFrame] | None:
    """Build `silver_timesheets` and its quarantine for `batch_date`. None if no Bronze rows."""
    lakehouse = get_lakehouse_root(lakehouse_root)
    resolved_run_id = run_id or f"manual-{batch_date}-{uuid.uuid4().hex[:8]}"
    bronze_path = join_path(lakehouse, "bronze", "timesheets")
    scd2_path = join_path(lakehouse, "silver", "silver_employees_scd2")
    silver_path = join_path(lakehouse, "silver", TABLE_NAME)
    quarantine_path = join_path(lakehouse, "silver_quarantine", "timesheets")
    audit_path = join_path(lakehouse, "dq", "dq_audit_results")

    bronze_day = (
        spark.read.format("delta")
        .load(bronze_path)
        .filter(F.col("_batch_date") == F.to_date(F.lit(batch_date)))
    )
    bronze_count = bronze_day.count()
    if bronze_count == 0:
        print(f"[{TABLE_NAME}] no Bronze rows for {batch_date}, skipping")
        return None

    scd2_df = spark.read.format("delta").load(scd2_path)
    evaluated = (
        evaluate(deduplicate(bronze_day), scd2_df)
        .withColumn("_run_id", F.lit(resolved_run_id))
        .withColumn("_silver_updated_at", F.current_timestamp())
        .cache()
    )
    valid_df = evaluated.filter(F.size("dq_reasons") == 0).drop("dq_reasons")
    quarantine_df = evaluated.filter(F.size("dq_reasons") > 0)

    write_daily(valid_df, silver_path, batch_date)
    write_daily(quarantine_df, quarantine_path, batch_date)

    results = timesheet_checks(
        valid_df=valid_df,
        quarantine_df=quarantine_df,
        bronze_day=bronze_day,
        batch_date=batch_date,
        run_id=resolved_run_id,
    )
    write_audit_results(spark, results, audit_path)
    evaluated.unpersist()
    enforce_dq_gate(results)
    return valid_df, quarantine_df


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Transform bronze/timesheets into silver_timesheets."
    )
    parser.add_argument("--batch-date", required=True, help="Logical batch date (YYYY-MM-DD).")
    parser.add_argument("--lakehouse-root", default=None, help="Override Lakehouse root path.")
    parser.add_argument("--run-id", default=None, help="Orchestrator run identifier.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = get_spark_session("silver-transform-timesheets")
    try:
        out = run(spark, args.batch_date, args.lakehouse_root, args.run_id)
        if out is not None:
            valid_df, quarantine_df = out
            print(
                f"Silver timesheets batch_date={args.batch_date}: "
                f"valid={valid_df.count()}, quarantined={quarantine_df.count()}"
            )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
