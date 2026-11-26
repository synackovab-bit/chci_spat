# Bronze Handoff (for your Claude Code)

Read `CLAUDE.md`, `docs/BRONZE_PLAN.md` and `docs/STUDENT_ASSIGNMENT.md` §3 and §4.2.A first. Work only on your own file; don't touch teammates' deliverables.

## Goal
Implement your Bronze job(s) so all 10 landing datasets land in `lakehouse/bronze/<table>` as Delta, idempotently, with zero lossy transformation. Reference implementation: `spark_jobs/bronze/ingest_clients.py`.

## Shared helper (already written, branch `feat/bronze-io`, tests pass)
`spark_jobs/common/bronze_io.py`. Branch from `feat/bronze-io` (or `main` once merged).

```python
from spark_jobs.common.bronze_io import add_metadata, path_exists, write_daily
from spark_jobs.common.spark_session import get_landing_root, get_lakehouse_root, join_path

src = join_path(landing_root, "invoices", f"date={batch_date}", "part-000.jsonl")
if not path_exists(spark, src):
    print(f"No file for {batch_date}, skipping"); return
df = add_metadata(spark.read.schema(SCHEMA).json(src), batch_date)   # call right after the read
write_daily(df, join_path(lakehouse_root, "bronze", "invoices"), batch_date)
```
- `write_daily` = Delta `replaceWhere _batch_date=...`, partitioned by `_batch_date`; re-runs don't duplicate.
- Skip (don't write an empty frame) when the source is missing, otherwise that day's rows get deleted.
- Full-load tables (`projects`, `survey_questions`): overwrite like `ingest_clients.py`.

## Job contract (keep the skeleton signatures)
`run(spark, batch_date, landing_root=None, lakehouse_root=None)` plus the existing argparse `main` (`--batch-date`, `--landing-root`, `--lakehouse-root`). Don't change them: the Airflow DAG calls them.

## Assignments and expected rows (days 2026-01-01..14)
| Owner | File | Tables -> rows |
|---|---|---|
| Simon | `bronze/ingest_hr.py` | employees 16,800; timesheets 34,675 |
| Eliska | `bronze/ingest_finance.py` | fx_rates 60; invoices 4,445; expenses 6,894 |
| Anna | `bronze/ingest_crm_surveys.py`, `bronze/ingest_reference.py` | crm_events 42,507; employee_surveys 760; projects 800; survey_questions 10 |
| Krystof | backfill + idempotency check, demo/reporting prep | |
| Barbora | DAG `bronze_ingest` tasks, `dq_bronze_gate` | |

## Gotchas
- employees CSV: explicit schema per §3.2; keep dates raw.
- fx_rates: JSON array -> `multiLine=true`; weekends have no file (skip).
- invoices: `issued_at`/`paid_at` stay strings (mixed `YYYY-MM-DD` / `DD.MM.YYYY`).
- expenses, timesheets, employee_surveys: keep duplicates and bad values (-1/11 scores, hours<=0); cleaning is Silver.
- crm_events: `payload` varies by event type; declare `StringType` (raw JSON) and verify it isn't null; fall back to reading as text if it is.
- employee_surveys: only on wave days; keep `answers` as `array<struct<question_id, dimension, numeric_score, written_answer>>`.
- Call `add_metadata` directly after the read (`input_file_name()` breaks after shuffles).

## Test and verify (Docker; native Windows has winutils issues)
```bash
docker compose run --rm spark-master pytest tests/test_bronze_<domain>.py -v   # copy tests/test_bronze_io.py style
uvx --system-certs ruff check . && uvx --system-certs ruff format --check .       # CI enforces, line length 100
for d in 2026-01-{01..14}; do docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 /opt/workspace/spark_jobs/bronze/<job>.py --batch-date $d; done
```
Compare counts to `docs/SANITY_CHECKS.md` §1.A, run the loop again, counts must be identical.

## Git
Branch `feat/bronze-<domain>`, small PR, no `.env` or `data/` commits. Commit message ends with the attribution line your Claude Code session specifies.
