"""Unit tests for Bronze CRM & survey ingestion (`crm_events`, `employee_surveys`)."""

from __future__ import annotations

import json
from pathlib import Path

from pyspark.sql import SparkSession

from spark_jobs.bronze.ingest_crm_surveys import run


def _write_jsonl(landing: Path, table: str, day: str, records: list[dict]) -> None:
    d = landing / table / f"date={day}"
    d.mkdir(parents=True, exist_ok=True)
    lines = "\n".join(json.dumps(r) for r in records) + "\n"
    (d / "part-000.jsonl").write_text(lines, encoding="utf-8")


def _answers(q01: int) -> list[dict]:
    out = [
        {"question_id": "Q01", "dimension": "enps", "numeric_score": q01, "written_answer": None}
    ]
    out += [
        {
            "question_id": f"Q{i:02d}",
            "dimension": f"dim_{i}",
            "numeric_score": 3,
            "written_answer": "ok",
        }
        for i in range(2, 11)
    ]
    return out


def _seed_crm(landing: Path, day: str) -> None:
    _write_jsonl(
        landing,
        "crm_events",
        day,
        [
            {
                "event_id": "EVT-1",
                "client_id": "VIP_CORP_001",
                "event_type": "api_call",
                "timestamp": f"{day}T00:01:00Z",
                "payload": {"endpoint": "/v1/metrics", "status_code": 200, "latency_ms": 123},
            },
            {
                "event_id": "EVT-2",
                "client_id": "CLI_0002",
                "event_type": "contract_renewal",
                "timestamp": f"{day}T00:02:00Z",
                "payload": {"opportunity_id": "OPP-1", "arr_delta_eur": 1500.5},
            },
        ],
    )


def _seed_surveys(landing: Path, day: str) -> None:
    record = {
        "survey_id": "SRV-1",
        "emp_id": "EMP_0001",
        "submitted_at": f"{day}T08:00:00Z",
        "enps_score": 11,  # out of range, kept in Bronze
        "overall_comment": "fine",
        "answers": _answers(11),
    }
    _write_jsonl(landing, "employee_surveys", day, [record, record])  # duplicate kept


def test_crm_payload_kept_as_raw_json(spark: SparkSession, tmp_path: Path) -> None:
    landing, lake = tmp_path / "landing", tmp_path / "lake"
    _seed_crm(landing, "2026-01-05")
    _seed_surveys(landing, "2026-01-05")

    counts = run(spark, "2026-01-05", str(landing), str(lake))
    assert counts == {"crm_events": 2, "employee_surveys": 2}

    crm = spark.read.format("delta").load(str(lake / "bronze" / "crm_events"))
    row = crm.filter("event_id = 'EVT-2'").first()
    assert json.loads(row["payload"]) == {"opportunity_id": "OPP-1", "arr_delta_eur": 1500.5}
    assert row["timestamp"] == "2026-01-05T00:02:00Z"  # raw string, not parsed
    assert str(row["_batch_date"]) == "2026-01-05"
    assert row["_source_file"].endswith("part-000.jsonl")
    assert crm.filter("payload IS NULL").count() == 0


def test_surveys_keep_nested_answers_and_bad_values(spark: SparkSession, tmp_path: Path) -> None:
    landing, lake = tmp_path / "landing", tmp_path / "lake"
    _seed_crm(landing, "2026-01-05")
    _seed_surveys(landing, "2026-01-05")
    run(spark, "2026-01-05", str(landing), str(lake))

    surveys = spark.read.format("delta").load(str(lake / "bronze" / "employee_surveys"))
    assert surveys.count() == 2
    row = surveys.first()
    assert row["enps_score"] == 11
    assert len(row["answers"]) == 10
    assert row["answers"][0]["written_answer"] is None
    assert row["answers"][1]["numeric_score"] == 3


def test_idempotent_and_skips_non_wave_day(spark: SparkSession, tmp_path: Path) -> None:
    landing, lake = tmp_path / "landing", tmp_path / "lake"
    _seed_crm(landing, "2026-01-05")
    _seed_surveys(landing, "2026-01-05")
    _seed_crm(landing, "2026-01-06")  # no survey file on this day

    run(spark, "2026-01-05", str(landing), str(lake))
    counts = run(spark, "2026-01-06", str(landing), str(lake))
    run(spark, "2026-01-05", str(landing), str(lake))

    assert counts == {"crm_events": 2, "employee_surveys": None}
    crm = spark.read.format("delta").load(str(lake / "bronze" / "crm_events"))
    surveys = spark.read.format("delta").load(str(lake / "bronze" / "employee_surveys"))
    assert crm.count() == 4
    assert surveys.count() == 2
