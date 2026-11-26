"""Small DQ helpers shared by the HR Silver jobs (`silver_employees_scd2`, `silver_timesheets`).

They build `DQCheckResult` rows for `dq.framework.write_audit_results` / `enforce_dq_gate`.
The team-wide reusable rule suite lives in `dq/checks.py`; these helpers keep the HR jobs
self-contained until that suite is available.
"""

from __future__ import annotations

from datetime import date, datetime, timezone

from dq.framework import DQCheckResult


def make_result(
    *,
    run_id: str,
    batch_date: str,
    table_name: str,
    check_name: str,
    severity: str,
    failed_count: int,
    total_count: int,
    passed: bool | None = None,
    layer: str = "silver",
) -> DQCheckResult:
    """Build one audit row. `passed` defaults to `failed_count == 0`.

    A failing ERROR check gets status FAIL (blocks the gate); a failing WARN check gets WARN.
    """
    ok = failed_count == 0 if passed is None else passed
    severity = severity.upper()
    status = "PASS" if ok else ("WARN" if severity == "WARN" else "FAIL")
    return DQCheckResult(
        run_id=run_id,
        batch_date=date.fromisoformat(batch_date),
        layer=layer,
        table_name=table_name,
        check_name=check_name,
        severity=severity,
        status=status,
        failed_count=int(failed_count),
        total_count=int(total_count),
        checked_at=datetime.now(timezone.utc),
    )


def ratio_result(
    *,
    run_id: str,
    batch_date: str,
    table_name: str,
    check_name: str,
    failed_count: int,
    total_count: int,
    max_ratio: float,
) -> DQCheckResult:
    """Soft (WARN) check that passes while `failed_count / total_count <= max_ratio`."""
    ratio = failed_count / total_count if total_count else 0.0
    return make_result(
        run_id=run_id,
        batch_date=batch_date,
        table_name=table_name,
        check_name=check_name,
        severity="WARN",
        failed_count=failed_count,
        total_count=total_count,
        passed=ratio <= max_ratio,
    )
