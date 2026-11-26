# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

Student starter repo for a Spark Lakehouse / Medallion course project (Bronze → Silver → Gold on Delta Lake, orchestrated by Airflow, served via DuckDB + Streamlit). Only the `clients` Bronze→Silver pipeline, the DQ framework, the R2 downloader, and CI are fully implemented. Everything else is a `TODO` skeleton that students fill in. The spec lives in `docs/STUDENT_ASSIGNMENT.md` (layer requirements §4.2, DQ §4.3, orchestration §4.4, reporting §4.5); data sanity checks and KPI definitions are in `docs/SANITY_CHECKS.md`. `docs/architecture.md` and `docs/runbook.md` are student deliverables.

## Team & roles

Five-person team; everyone uses this file. Roles come from `team_roles.txt` (untracked, repo root; re-read it for changes). The username in brackets, e.g. `(nike03156)`, is the person's GitHub username. Work in your own area, and don't implement or overwrite another member's deliverables unless asked. If it isn't clear who you're working with, ask (git user / GitHub username helps).

| Member (GitHub) | Area | Owns |
| :--- | :--- | :--- |
| Simon Bartek (`simonbartek`) | Data Ingestion & HR | All Bronze daily ingestion (`spark_jobs/bronze/*`), `silver_employees_scd2` (SCD2), `silver_timesheets` dedup/quarantine |
| Eliška Nikolova (`nike03156`) | **Finance** | `silver_fx_rates_daily`, `silver_invoices`, `silver_expenses` |
| Anna Snevajsova (`stupydo`) | CRM & eNPS Surveys | `silver_crm_events` (VIP_CORP_001 skew + Spark UI evidence), `silver_survey_responses` / `silver_survey_answers` |
| Krystof Zak (`sazk06`) | Gold, Reporting & Bonus ML | 5 Gold tables, Streamlit + DuckDB dashboard (4 views), optional churn/NLP bonus |
| Barbora Synackova (`synackovabab-bit`) | Orchestration, DQ, CI & Agent Skills | Airflow DAG + backfill idempotency, `dq/checks.py` + `dq_audit_results` gates, CI hygiene, team skills/plugins from `spark-lakehouse-marketplace` |

### Finance Domain details (Eliška)

- **Landing data (spec §3.4):** `fx_rates` (JSON, weekdays only; 6 pairs among EUR/USD/CZK), `invoices` (JSONL; mixed `YYYY-MM-DD` and `DD.MM.YYYY`; negative amounts are **credit notes**, not errors; `paid_at = NULL` means unpaid; `PRJ_ORPHAN_*` project ids are orphans), `expenses` (CSV; duplicates; 7 categories; EUR/USD/CZK). `projects` (`budget_eur`) and `clients` feed the invoice joins.
- **Jobs:** `silver/transform_fx_rates.py` (calendar spine + LOCF via `last(..., ignorenulls=True)`) and `silver/transform_invoices_expenses.py` (EU date parsing, FX → `amount_eur`, orphan quarantine to `silver_quarantine.*` with `dq_reasons`, `is_credit_note`/`net_amount_eur`). Silver order: fx_rates before invoices/expenses.
- **Dependencies on teammates:** Simon's Bronze finance tables (`ingest_finance.py`); Krystof's Gold marts consume her Silver outputs, so keep column names stable and per the spec; Barbora's DQ gates and DAG tasks wrap these jobs.
- **Current focus is analyzing the data:** profile `data/landing` directly (pandas/DuckDB/Spark) before building pipeline code.

## Commands

Spark/Delta tests are meant to run in Docker (avoids Windows `winutils`/JVM issues); CI runs them natively with Java 17.

```bash
cp .env.example .env && docker compose up -d --build     # stack (add --profile minio for local S3)
docker compose run --rm spark-master pytest -v           # full test suite in container
uv sync --frozen --dev && uv run pytest -v               # native (needs Java 17)
uv run pytest tests/test_download_data.py -v             # single file (no Spark needed)
uv run pytest tests/test_clients_pipeline.py::<test> -v  # single test

# Lint/format (CI enforces all of these; ruff line-length 100)
uv lock --check
uv run ruff check . && uv run ruff format --check .
bash -n scripts/download_data.sh && docker compose config --quiet

# Data download (Cloudflare R2, manifest + SHA-256 verified; needs R2_* vars in .env)
./scripts/download_data.sh        # or .\scripts\download_data.ps1 ; --verify-only to re-check locally

# Run a Spark job (paths are container paths)
docker compose exec spark-master spark-submit --master spark://spark-master:7077 \
  /opt/workspace/spark_jobs/bronze/ingest_clients.py --batch-date 2026-01-01
```

CI (`.github/workflows/ci.yml`) also runs Gitleaks, Semgrep, and Trivy on pushes/PRs to `main` and `develop`. Branching follows `feat/*` → `develop` → `main` with release commits.

## Architecture

- **Config via env vars**: `LANDING_ROOT`, `LAKEHOUSE_ROOT`, `SPARK_MASTER_URL`, `SPARK_SHUFFLE_PARTITIONS`, `SPARK_EVENT_LOG_DIR`, optional `S3_ENDPOINT_URL` (switches on S3A/MinIO config). Defaults are local (`./data/landing`, `./lakehouse`, `local[*]`). All Spark jobs get their session from `spark_jobs/common/spark_session.py::get_spark_session` and build paths with `join_path` (works for both local paths and `s3a://` URIs). It uses preinstalled Delta JARs when present, else `configure_spark_with_delta_pip`.
- **Job pattern** (copy `spark_jobs/bronze/ingest_clients.py` and `silver/transform_clients.py`): each job exposes a function taking `(spark, batch_date, landing_root, lakehouse_root)` plus an argparse `main()` with `--batch-date`, `--landing-root`, `--lakehouse-root` (Silver also `--run-id`). Bronze uses explicit raw schemas and adds `_batch_date`, `_ingested_at`, `_source_file`. `clients` is a full overwrite; daily tables must be partitioned by `_batch_date` and rewritten idempotently for just that date (`replaceWhere` or delete+append). Lakehouse layout: `{LAKEHOUSE_ROOT}/{bronze|silver|gold}/<table>`.
- **DQ** (`dq/framework.py`, `dq/checks.py`): checks produce `DQCheckResult` rows written to the `dq_audit_results` Delta table; hard (`ERROR`) failures raise `DQGateFailedError` to fail the Airflow gate, soft (warn) ones only log. `checks.py` is TODO skeletons.
- **Airflow** (`dags/medallion_daily_dag.py`): DAG `medallion_daily_pipeline` runs `spark-submit` via `BashOperator` with `{{ ds }}`/`{{ run_id }}`. Graph: `sense_landing_files >> bronze_ingest >> dq_bronze_gate >> silver_transform >> dq_silver_gate >> gold_marts >> dq_gold_gate >> refresh_reporting_layer`. Only `clients` tasks are real; the rest are `EmptyOperator` `*_todo` placeholders to replace. Silver dependencies: employees→timesheets, fx_rates→invoices/expenses, clients→crm_events.
- **Reporting** (`reporting/db.py`, `reporting/app.py`): DuckDB `delta_scan` over Gold tables, Streamlit app on :8501 (Home page done, 4 view tabs TODO).
- **Docker** (`docker/{spark,airflow,reporting}`): Spark 3.5.5 + Delta 3.3.0 + hadoop-aws 3.3.4; Airflow 2.10.5 LocalExecutor with PySpark. Repo is mounted at `/opt/workspace`.
- **Data**: 200 dev days (2026-01-01 – 2026-07-19) downloaded to `./data/landing` (gitignored); a 70-day holdout (2026-07-20 – 2026-09-27) is reserved for grading. `scripts/download_data.py` reads `MANIFEST.sha256` from R2 (`R2_PREFIX=course-2026`) and checks all 10 required dataset folders.
- **Tests**: `tests/conftest.py` provides a session-scoped local Delta `spark` fixture (`local[2]`, UI and event log off). `pytest` has `pythonpath = ["."]`, so import as `spark_jobs.…`, `dq.…`.
