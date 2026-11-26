"""Bronze ingestion for full-load reference datasets: `projects` and `survey_questions`.

- `<landing_root>/projects/projects.csv` (header, explicit schema)
- `<landing_root>/survey_questions/survey_questions.json` (one multiline JSON array)

Both are full loads, so the Delta table is overwritten on every run (idempotent), following
`ingest_clients.py`. `_batch_date` records the run that produced the current snapshot; older
snapshots stay reachable through Delta time travel. Reads use FAILFAST so a malformed value
fails the run instead of silently becoming NULL.

Reference: `docs/STUDENT_ASSIGNMENT.md` (Sections 3.1, 3.3, 4.2.A) and `docs/SANITY_CHECKS.md`.
"""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from spark_jobs.common.bronze_io import add_metadata
from spark_jobs.common.spark_session import (
    get_lakehouse_root,
    get_landing_root,
    get_spark_session,
    join_path,
)

PROJECTS_RAW_SCHEMA = StructType(
    [
        StructField("project_id", StringType(), nullable=True),
        StructField("client_id", StringType(), nullable=True),
        StructField("budget_eur", DoubleType(), nullable=True),
        StructField("status", StringType(), nullable=True),
    ]
)

SURVEY_QUESTIONS_RAW_SCHEMA = StructType(
    [
        StructField("question_id", StringType(), nullable=True),
        StructField("dimension", StringType(), nullable=True),
        StructField("scale_min", IntegerType(), nullable=True),
        StructField("scale_max", IntegerType(), nullable=True),
        StructField("prompt_text", StringType(), nullable=True),
    ]
)


def _overwrite(df: DataFrame, target_path: str) -> int:
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(target_path)
    return df.count()


def ingest_projects(
    spark: SparkSession, batch_date: str, landing_root: str, lakehouse_root: str
) -> int:
    src = join_path(landing_root, "projects", "projects.csv")
    raw = (
        spark.read.option("header", "true")
        .option("mode", "FAILFAST")
        .schema(PROJECTS_RAW_SCHEMA)
        .csv(src)
    )
    return _overwrite(
        add_metadata(raw, batch_date), join_path(lakehouse_root, "bronze", "projects")
    )


def ingest_survey_questions(
    spark: SparkSession, batch_date: str, landing_root: str, lakehouse_root: str
) -> int:
    src = join_path(landing_root, "survey_questions", "survey_questions.json")
    raw = (
        spark.read.option("multiLine", "true")
        .option("mode", "FAILFAST")
        .schema(SURVEY_QUESTIONS_RAW_SCHEMA)
        .json(src)
    )
    return _overwrite(
        add_metadata(raw, batch_date), join_path(lakehouse_root, "bronze", "survey_questions")
    )


def run(
    spark: SparkSession,
    batch_date: str,
    landing_root: str | None = None,
    lakehouse_root: str | None = None,
) -> dict[str, int]:
    """Overwrite `bronze/projects` and `bronze/survey_questions` with the current full loads."""
    landing = get_landing_root(landing_root)
    lakehouse = get_lakehouse_root(lakehouse_root)
    return {
        "projects": ingest_projects(spark, batch_date, landing, lakehouse),
        "survey_questions": ingest_survey_questions(spark, batch_date, landing, lakehouse),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest reference datasets into Bronze Delta.")
    parser.add_argument("--batch-date", required=True, help="Logical batch date (YYYY-MM-DD).")
    parser.add_argument("--landing-root", default=None, help="Override Landing Zone path.")
    parser.add_argument("--lakehouse-root", default=None, help="Override Lakehouse root path.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = get_spark_session("bronze-ingest-reference")
    try:
        counts = run(spark, args.batch_date, args.landing_root, args.lakehouse_root)
        print(f"Bronze reference batch_date={args.batch_date}: {counts}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
