# Bronze Tier Plan (one-day sprint)

Spec: `docs/STUDENT_ASSIGNMENT.md` §4.2.A. Expected counts: `docs/SANITY_CHECKS.md` §1.A.

## Definition of done
- All 10 datasets land in `lakehouse/bronze/<table>` as Delta.
- Every table has `_batch_date`, `_ingested_at`, `_source_file`.
- Raw values untouched (invoice date strings, nested JSON payloads, duplicates, bad scores all kept).
- Idempotent: re-running a `batch_date` creates no duplicates.
- Missing files (weekend `fx_rates`, non-wave days for `employee_surveys`) are skipped, not fatal.

## Who does what
Roles give all Bronze to Simon. The four job files are independent, so we split by file (Simon to confirm).

| Who | Does today | Done when (days 1-14) |
|---|---|---|
| Simon | `spark_jobs/common/bronze_io.py` first (~30 min), then `ingest_hr.py` | employees 16,800; timesheets 34,675 |
| Eliska | `ingest_finance.py` | fx_rates 60; invoices 4,445; expenses 6,894 |
| Anna | `ingest_crm_surveys.py` + `ingest_reference.py` | crm_events 42,507; employee_surveys 760; projects 800; survey_questions 10 |
| Krystof | 14-day backfill + idempotency check once merged; architecture diagram and reporting scaffolding for the demo | verification passes |
| Barbora | Replace the `bronze_ingest` `EmptyOperator` in the DAG; `dq_bronze_gate` checks in `dq/checks.py`; keep CI green | DAG parses; non-empty-partition (fx_rates skipped on weekends), schema and `_batch_date` checks |

## Shared helper (`spark_jobs/common/bronze_io.py`)
1. `add_metadata(df, batch_date)` adds the three `_` columns.
2. `write_daily(df, path, batch_date)` does a Delta `replaceWhere` on `_batch_date`, partitioned by `_batch_date`.
3. `path_exists(spark, path)` checks via the Hadoop FileSystem API, so jobs can skip missing days.

## Per-dataset gotchas
- employees (CSV): explicit schema from §3.2; dates may stay strings.
- timesheets (Parquet): read as is.
- fx_rates: JSON array, needs `multiLine=true`; skip weekends via `path_exists`.
- invoices: `issued_at` / `paid_at` stay `StringType`; no parsing in Bronze.
- expenses (CSV): keep duplicates; dedup is Silver.
- crm_events: `payload` as `StringType` (raw JSON text). Verify it is not null; if it is, read as text and extract top-level fields.
- employee_surveys: keep `answers` as `array<struct<...>>`; keep out-of-range scores (-1, 11).
- projects, survey_questions: full load, overwrite like `clients`; survey_questions is a multiline JSON array.

## Timeline (~4 h)
1. 0:00-0:30: Simon pushes `bronze_io.py`; others branch `feat/bronze-<domain>` and profile their data.
2. 0:30-1:30: implement each job, run for `2026-01-05`.
3. 1:30-2:00: one test per job (`tests/test_bronze_<domain>.py`, copy `test_clients_pipeline.py`), `ruff check` + `ruff format`, PRs to `develop`.
4. 2:00-2:45: merge, Krystof runs the 14-day backfill, then re-runs `2026-01-05` and confirms counts are unchanged.
5. 2:45-4:00: DAG + DQ gate demo; slides (architecture, table list, idempotency proof, `_source_file` / `_batch_date` evidence).

## Verify (in Docker)
```bash
for d in 2026-01-{01..14}; do
  docker compose exec spark-master spark-submit --master spark://spark-master:7077 \
    /opt/workspace/spark_jobs/bronze/ingest_finance.py --batch-date $d
done
```
Compare counts to SANITY_CHECKS §1.A, repeat the loop, show counts identical (idempotency proof).

## Risks
- Use Docker, not native Windows (winutils).
- Only the helper is shared; merge it first, everyone else edits only their own file.
- No Silver work until Bronze is merged.
