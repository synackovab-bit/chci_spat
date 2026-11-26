# Finance Landing Data Profiling

Scope: `fx_rates`, `invoices`, `expenses` (plus `projects`/`clients` for joins).
Profiled directly from `data/landing` (dev window 2026-01-01 – 2026-07-19) with DuckDB, read-only.
Owner: Eliška Nikolova (Finance Silver tables).

## 1. fx_rates (JSON array per day)

| Check | Result |
| :--- | :--- |
| Files / rows | 142 files (one per weekday, 2026-01-01 – 2026-07-17), 6 rows each = 852 rows |
| Pairs | EUR↔USD, EUR↔CZK, USD↔CZK (6 directed pairs), 142 rows each |
| Duplicates / nulls / rate <= 0 | none |
| Weekends | no files (58 days in the window); every weekday present |
| Folder date vs `rate_date` | always equal |
| Consistency | EUR→USD x USD→EUR = 1.0 (+/-1e-6); no day-over-day move > 3% |

Implications:
- Calendar spine must run to the batch date (dev window ends Sun 2026-07-19; last rate is Fri 07-17) and carry rates forward with `last(rate, ignorenulls=True)`.
- ~5,800 invoices and ~7,600 expenses fall on non-FX days, so the fill-forward is required for the EUR join.
- Add an `is_filled` flag to mark carried-forward rows.

## 2. invoices (JSONL)

62,480 rows, 200 files, no duplicate `inv_id`, no nulls except `paid_at`.

| Check | Result |
| :--- | :--- |
| `issued_at` format | 56,044 `YYYY-MM-DD`, 6,436 `DD.MM.YYYY`; none unparsable |
| `paid_at` format | 39,259 ISO, 4,411 `DD.MM.YYYY`, 18,810 null (30%, unpaid) |
| Ambiguity | 2,707 `DD.MM.YYYY` values have day <= 12, so use an explicit `dd.MM.yyyy` pattern, never guess |
| `issued_at` vs folder date | always equal |
| `paid_at` < `issued_at` | none; days-to-pay 3–45 (avg 24) |
| `paid_at` after 2026-07-19 | 5,180 (valid; handle in "paid as of date" KPIs) |
| Credit notes (amount < 0) | 6,250 (10%) across EUR/USD/CZK; 4,328 have `paid_at`, 1,922 do not |
| Orphan projects | 648 rows with `PRJ_ORPHAN_*`; the only project ids missing from `projects` |
| Client join | all `client_id` exist in `clients` (VIP_CORP_001: 732 invoices); invoice client always equals the project's client |
| Currency | EUR 34,552 / USD 15,502 / CZK 12,426; CZK amounts reach +/-2.7M |

## 3. expenses (CSV)

101,491 rows, 200 files, 92,264 distinct `exp_id`.

| Check | Result |
| :--- | :--- |
| Nulls / empties | none |
| Date format | all `YYYY-MM-DD`; equals folder date; `exp_id` date prefix matches `expense_date` |
| Categories (7) | Cloud Infrastructure, Software Licenses, Travel & Lodging, Subcontractors, Hardware, Training, Office & Events |
| Departments (6) | Engineering, Data, Consulting, Sales, Operations, Finance |
| Currency | EUR 55,705 / USD 25,318 / CZK 20,468; all amounts positive, no whitespace/case issues |
| Duplicate `exp_id` | 9,003 ids, 9,227 extra rows (8,779 twice, 224 three times); all within the same day file |
| Duplicate type | 4,383 exact repeats; 4,620 differ **only in `amount`** (small drift, e.g. 380.61 vs 393.37) |
| Position | extra copies are appended after the day's original rows |

## 4. projects / clients

- `projects`: 800 unique ids, no null `budget_eur`; status Active 426, Completed 159, Planned 134, OnHold 81.
- `clients`: all invoice `client_id` values resolve.

## 5. Decisions for Silver

1. **FX (`silver_fx_rates_daily`)**: calendar spine from 2026-01-01 through the batch date, LOCF per currency pair, `is_filled` flag.
2. **Invoices (`silver_invoices`)**:
   - parse dates with `coalesce(to_date(x,'yyyy-MM-dd'), to_date(x,'dd.MM.yyyy'))`;
   - `is_credit_note = amount < 0`; credit notes **stay as ordinary rows** (including those without `paid_at`);
   - `is_paid = paid_at IS NOT NULL`;
   - `amount_eur` via the FX rate for `issued_at`; `net_amount_eur` is the signed value;
   - orphan projects go to `silver_quarantine.*` with `dq_reasons` (e.g. `orphan_project`).
3. **Expenses (`silver_expenses`)**:
   - **dedupe on `exp_id`, keeping the first occurrence** (original row); discarded copies go to quarantine with `dq_reasons = duplicate_exp_id`;
   - "first" needs a stable order, but Bronze keeps no row-order field (confirmed by Simon); see the open item in section 6;
   - `amount_eur` via the FX rate for `expense_date`.
4. **Order**: fx_rates before invoices/expenses.

## 6. Open items and confirmations

### Resolved: DQ coverage (Barbora)

Bronze (`dq_bronze_gate`, done): fx_rates (weekday-only `non_empty_partition`), invoices and expenses have `table_exists`, `expected_schema`, `non_empty_partition` and `_batch_date` / metadata checks (ERROR). Duplicates and orphan ids stay raw in Bronze.

Silver (`dq_silver_gate`) for the Finance tables:

| Concern | Check | Table(s) | Severity |
| :--- | :--- | :--- | :--- |
| FX coverage | `check_zero_null_eur` | `silver_invoices`, `silver_expenses` | ERROR |
| FX coverage | `check_non_empty_partition` (`allow_weekends_empty=False`) | `silver_fx_rates_daily` | ERROR |
| FX coverage | `check_not_null` on `rate` | `silver_fx_rates_daily` | ERROR |
| FX uniqueness | `check_unique_key` on `(rate_date, currency_from, currency_to)` | `silver_fx_rates_daily` | ERROR |
| Orphan rate | `check_quarantine_ratio` (`max_ratio=0.08`, or a dedicated `orphan_rate` threshold) | `silver_invoices` + `silver_quarantine/invoices` | WARN |
| Duplicate rate | `check_duplicate_rate` before/during dedup | `silver_expenses` | WARN |
| Key uniqueness | `check_unique_key` on `exp_id` / `inv_id` post-dedup | `silver_expenses`, `silver_invoices` | ERROR |

Sanity check on the thresholds against the profiling above:
- Orphan rate is 648 / 62,480 = **1.04%**, well under the 0.08 limit.
- Duplicate rate is 9,227 / 101,491 = **9.1%** of raw expense rows, so the duplicate-rate WARN will fire on dev data unless its threshold is set above ~10%. The Silver job should pass `raw_count` / `deduped_count` (or `valid_count` / `quarantined_count`) to the check.
- `check_non_empty_partition` with `allow_weekends_empty=False` is correct for Silver FX only because the calendar spine fills weekends.

### Open: expense dedupe tie-break (Simon)

Simon confirmed Bronze **does not keep a row-order field**. "Keep the first copy" therefore cannot be derived from Bronze, and Delta/Spark gives no ordering guarantee on read. Within a duplicate group, `_batch_date`, `_source_file`, department, category, currency and date are identical, so only `amount` differs. Options:

1. **Ask Simon to add a row index** to `ingest_finance.py` for expenses (e.g. a `_row_id` assigned per source file at read time). Simon has declined adding a row-order field, so this needs a renewed request with the reason: it is the only way to make "first copy" reproducible.
2. **Read the expenses CSV in Silver for ordering.** Rejected: it breaks the Bronze -> Silver layering.
3. **Deterministic fallback on Bronze columns** (e.g. keep the lowest `amount` per `exp_id`). Reproducible, but it is not "first copy" and has no support in the data. It would change the amounts that reach the Gold marts.

Until this is settled, the dedupe in `silver_expenses` is not implemented. The choice affects the amounts of 4,620 `exp_id`s; the 4,383 exact repeats are unaffected.
