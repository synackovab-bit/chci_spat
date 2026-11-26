"""Tests for Bronze Finance ingestion (fx_rates, invoices, expenses)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from spark_jobs.bronze.ingest_finance import run

FX = [
    {"rate_date": "2026-01-05", "currency_from": "EUR", "currency_to": "USD", "rate": 1.08},
    {"rate_date": "2026-01-05", "currency_from": "USD", "currency_to": "EUR", "rate": 0.92},
]
INVOICES = [
    {
        "inv_id": "I1",
        "client_id": "C1",
        "project_id": "PRJ_0001",
        "amount": 100.0,
        "currency": "EUR",
        "issued_at": "05.01.2026",
        "paid_at": None,
    },
    {
        "inv_id": "I2",
        "client_id": "C1",
        "project_id": "PRJ_ORPHAN_1",
        "amount": -50.0,
        "currency": "USD",
        "issued_at": "2026-01-05",
        "paid_at": "2026-01-09",
    },
]
EXPENSES_CSV = (
    "exp_id,department,category,amount,currency,expense_date\n"
    "E1,Data,Training,10.5,EUR,2026-01-05\n"
    "E1,Data,Training,10.5,EUR,2026-01-05\n"
)


def _write_day(landing: Path, day: str, *, fx: bool = True) -> None:
    if fx:
        p = landing / "fx_rates" / f"date={day}"
        p.mkdir(parents=True)
        (p / "part-000.json").write_text(json.dumps(FX, indent=2))
    p = landing / "invoices" / f"date={day}"
    p.mkdir(parents=True)
    (p / "part-000.jsonl").write_text("\n".join(json.dumps(r) for r in INVOICES) + "\n")
    p = landing / "expenses" / f"date={day}"
    p.mkdir(parents=True)
    (p / "part-000.csv").write_text(EXPENSES_CSV)


@pytest.fixture
def roots(tmp_path: Path) -> tuple[str, str]:
    landing = tmp_path / "landing"
    _write_day(landing, "2026-01-05")
    return str(landing), str(tmp_path / "lakehouse")


def _table(spark: SparkSession, lakehouse: str, name: str):
    return spark.read.format("delta").load(f"{lakehouse}/bronze/{name}")


def test_ingest_preserves_raw_values(spark: SparkSession, roots: tuple[str, str]) -> None:
    landing, lakehouse = roots
    written = run(spark, "2026-01-05", landing, lakehouse)
    assert written == {"fx_rates": 2, "invoices": 2, "expenses": 2}

    inv = {r["inv_id"]: r for r in _table(spark, lakehouse, "invoices").collect()}
    assert inv["I1"]["issued_at"] == "05.01.2026"  # raw European string untouched
    assert inv["I1"]["paid_at"] is None
    assert inv["I2"]["amount"] == -50.0  # credit note kept
    assert inv["I2"]["project_id"] == "PRJ_ORPHAN_1"  # orphan kept for Silver quarantine

    exp = _table(spark, lakehouse, "expenses")
    assert exp.count() == 2  # duplicates kept
    for table in ("fx_rates", "invoices", "expenses"):
        df = _table(spark, lakehouse, table)
        assert {"_batch_date", "_ingested_at", "_source_file"} <= set(df.columns)
        assert "part-000" in df.first()["_source_file"]


def test_rerun_is_idempotent(spark: SparkSession, roots: tuple[str, str]) -> None:
    landing, lakehouse = roots
    run(spark, "2026-01-05", landing, lakehouse)
    run(spark, "2026-01-05", landing, lakehouse)
    assert _table(spark, lakehouse, "fx_rates").count() == 2
    assert _table(spark, lakehouse, "invoices").count() == 2
    assert _table(spark, lakehouse, "expenses").count() == 2


def test_weekend_without_fx_file_is_skipped(spark: SparkSession, roots: tuple[str, str]) -> None:
    landing, lakehouse = roots
    _write_day(Path(landing), "2026-01-03", fx=False)  # a Saturday
    run(spark, "2026-01-05", landing, lakehouse)
    written = run(spark, "2026-01-03", landing, lakehouse)
    assert written["fx_rates"] is None
    assert written["invoices"] == 2
    assert _table(spark, lakehouse, "fx_rates").count() == 2  # earlier day untouched
