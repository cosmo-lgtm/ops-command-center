"""
KPI guardrails — catch silently-wrong prior-period comparisons before they ship.

The rookie mistake we keep making: trusting a precomputed "prior" aggregate
column (ttm_prior, qty_previous_90_days, etc.) without checking it's actually
populated. Result: dashboards render "0.0 / New" next to metrics where we
absolutely have history.

Usage pattern inside a Streamlit page:

    from kpi_guard import validate_kpis, KpiCheck

    checks = [
        KpiCheck("Depletions", current=depl_current, prior=depl_prior,
                 source="retail_customer_fact_sheet_2026.qty_last_90_days / qty_previous_90_days"),
        KpiCheck("SKUs per Account", current=skus_per_acct, prior=skus_per_acct_prior,
                 source="chain_sales_report_2026 monthly columns"),
        ...
    ]
    validate_kpis(checks)   # renders a red banner + logs if any check fails

Design rule enforced here:
  - If current > 0 but prior == 0 or None, the prior source is almost
    certainly broken or misaligned. Show a loud banner naming the KPI and
    source so the next reviewer catches it during QA instead of a customer
    catching it in Slack.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import streamlit as st


@dataclass
class KpiCheck:
    name: str
    current: float | int | None
    prior: float | int | None
    source: str = ""
    # Some KPIs legitimately have no prior signal (e.g. a brand-new metric).
    # Set allow_missing_prior=True to opt out of the symmetry check.
    allow_missing_prior: bool = False


def _is_populated(value) -> bool:
    if value is None:
        return False
    try:
        return float(value) != 0.0
    except (TypeError, ValueError):
        return False


def find_symmetry_breaks(checks: Iterable[KpiCheck]) -> list[KpiCheck]:
    """Return checks where current is populated but prior is not."""
    broken = []
    for c in checks:
        if c.allow_missing_prior:
            continue
        if _is_populated(c.current) and not _is_populated(c.prior):
            broken.append(c)
    return broken


def validate_kpis(checks: Iterable[KpiCheck]) -> bool:
    """Render a red banner if any KPI has current>0 but prior==0/None.

    Returns True when all checks pass, False when one or more failed. Callers
    can use the return value to block downstream rendering in stricter modes.
    """
    checks = list(checks)
    broken = find_symmetry_breaks(checks)
    if not broken:
        return True

    lines = "\n".join(
        f"- **{c.name}** — current={c.current}, prior={c.prior}"
        + (f" (source: `{c.source}`)" if c.source else "")
        for c in broken
    )
    st.error(
        "**KPI guardrail tripped — prior-period data is empty for one or more "
        "metrics.** This almost always means a precomputed 'prior' column is "
        "broken; derive prior from the same raw columns as current.\n\n"
        f"{lines}"
    )
    return False
