"""Unit tests for Bronze reference ingestion (`projects`, `survey_questions`)."""

from __future__ import annotations

import json
from pathlib import Path

from pyspark.sql import SparkSession

from spark_jobs.bronze.ingest_reference import run


def _seed(landing: Path) -> None:
    (landing / "projects").mkdir(parents=True, exist_ok=True)
    (landing / "projects" / "projects.csv").write_text(
        "project_id,client_id,budget_eur,status\n"
        "PRJ_0001,CLI_0009,12200.0,Active\n"
        "PRJ_0002,CLI_0064,105900.0,Planned\n",
        encoding="utf-8",
    )
    (landing / "survey_questions").mkdir(parents=True, exist_ok=True)
    questions = [
        {
            "question_id": "Q01",
            "dimension": "enps",
            "scale_min": 0,
            "scale_max": 10,
            "prompt_text": "Recommend?",
        },
        {
            "question_id": "Q02",
            "dimension": "work_life_balance",
            "scale_min": 1,
            "scale_max": 5,
            "prompt_text": "Balance?",
        },
    ]
    # Multiline JSON array, like the landing file.
    (landing / "survey_questions" / "survey_questions.json").write_text(
        json.dumps(questions, indent=2), encoding="utf-8"
    )


def test_reference_full_load_and_idempotent(spark: SparkSession, tmp_path: Path) -> None:
    landing, lake = tmp_path / "landing", tmp_path / "lake"
    _seed(landing)

    assert run(spark, "2026-01-05", str(landing), str(lake)) == {
        "projects": 2,
        "survey_questions": 2,
    }
    run(spark, "2026-01-06", str(landing), str(lake))  # full reload overwrites, no duplicates

    projects = spark.read.format("delta").load(str(lake / "bronze" / "projects"))
    questions = spark.read.format("delta").load(str(lake / "bronze" / "survey_questions"))
    assert projects.count() == 2
    assert questions.count() == 2

    prj = projects.filter("project_id = 'PRJ_0002'").first()
    assert prj["budget_eur"] == 105900.0
    assert str(prj["_batch_date"]) == "2026-01-06"
    assert prj["_source_file"].endswith("projects.csv")

    q01 = questions.filter("question_id = 'Q01'").first()
    assert (q01["scale_min"], q01["scale_max"]) == (0, 10)
