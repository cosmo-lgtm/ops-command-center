"""
Beverage Trends Dashboard
Social-media + web mention tracker for functional and THC/intoxicating beverages.
Data harvested 3x/day by scripts/beverage-trends/harvest.py via SearXNG.
Designed per spec docs/superpowers/specs/2026-04-07-beverage-trends-dashboard-design.md
"""

from __future__ import annotations

from datetime import datetime

import pandas as pd
import streamlit as st
from google.cloud import bigquery

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Beverage Trends",
    page_icon="🥤",
    layout="wide",
    initial_sidebar_state="collapsed",
)

BQ_PROJECT = "artful-logic-475116-p1"
DATASET = "beverage_trends"

# ---------------------------------------------------------------------------
# CSS — Jost font + Nowadays brand palette, scoped to this page
# ---------------------------------------------------------------------------

NOWADAYS_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Jost:wght@300;400;500;600;700&family=Material+Symbols+Outlined:opsz,wght,FILL,GRAD@24,400,0,0&display=swap');

:root {
  /* Nowadays brand accents (per Lisa's spec) */
  --cream:  #E7B78A;
  --mist:   #D7D2CB;
  --white:  #FFFFFF;
  --char:   #2D2926;
  --yellow: #F4C864;
  --pink:   #FE99A9;
  --green:  #85C79D;
  --sky:    #8EDDED;
  --forest: #3F634E;
  --navy:   #074A7A;

  /* Editorial surface canvas (warmer than pure white — harmonizes with cream) */
  --bt-surface: #fef9f1;
  --bt-surface-lowest: #ffffff;
  --bt-surface-low: #f9f3ea;
  --bt-surface-container: #f3ede4;
  --bt-surface-high: #eee7dd;
  --bt-surface-variant: #e8e2d6;
  --bt-on-surface-variant: #625f56;
  --bt-outline: #7e7a71;
  --bt-outline-variant: #b7b1a7;

  /* Editorial shadow */
  --bt-shadow: 0 6px 24px rgba(45, 41, 38, 0.08);
  --bt-shadow-lg: 0 12px 36px rgba(45, 41, 38, 0.12);
}

/* Material Symbols icon rendering */
.material-symbols-outlined {
  font-family: 'Material Symbols Outlined' !important;
  font-weight: normal;
  font-style: normal;
  font-size: 24px;
  line-height: 1;
  letter-spacing: normal;
  text-transform: none;
  display: inline-block;
  white-space: nowrap;
  word-wrap: normal;
  direction: ltr;
  font-feature-settings: 'liga';
  -webkit-font-smoothing: antialiased;
  font-variation-settings: 'FILL' 0, 'wght' 400, 'GRAD' 0, 'opsz' 24;
  vertical-align: middle;
}

/* Global Jost font override on this page only. Streamlit ships its own
   Source Sans theme font and applies it via class-hashed rules; we use
   `:where()` to wrap the wildcard so the rule has !important strength
   but ZERO specificity, letting any class-targeted size/weight rules
   below win without specificity wars. The Material Symbols icon font
   is excluded so glyphs render correctly. */
:where([data-testid="stApp"]),
:where([data-testid="stApp"] *):not(.material-symbols-outlined) {
  font-family: 'Jost', 'Helvetica', sans-serif !important;
}

/* Headlines + label-style elements use Jost. The .bt-* classes are
   high-specificity tags; we list everything explicitly so the rule
   beats the body-font wildcard above (same specificity, listed later). */
[data-testid="stApp"] h1,
[data-testid="stApp"] h2,
[data-testid="stApp"] h3,
[data-testid="stApp"] h4,
[data-testid="stApp"] h5,
[data-testid="stApp"] h6,
.bt-page-title,
.bt-page-subtitle,
.bt-hero-title,
.bt-hero-eyebrow,
.bt-hero-sub,
.bt-card-title,
.bt-card-eyebrow,
.bt-section-title,
.bt-drill-title,
.bt-refresh-eyebrow,
.bt-refresh-time,
.bt-rank,
.bt-sp-rank,
.bt-sp-score,
.bt-sp-entity-name,
.bt-sp-score-suffix,
.bt-discovery-phrase,
.bt-discovery-volume,
.bt-discovery-growth,
.bt-discovery-action,
.bt-engine-pill,
.bt-source-link,
.bt-source-title,
.bt-type-badge,
.bt-growth-pos,
.bt-growth-neg,
.bt-viral,
.bt-chip,
.bt-legend-item,
.bt-footer {
  font-family: 'Jost', 'Helvetica', sans-serif !important;
}

.material-symbols-outlined,
.material-symbols-outlined * {
  font-family: 'Material Symbols Outlined' !important;
}

[data-testid="stApp"] {
  background: linear-gradient(135deg, var(--mist) 0%, var(--bt-surface) 50%, var(--cream) 100%) !important;
  background-attachment: fixed !important;
}

/* IMPORTANT: do NOT override stMain's overflow / height. Streamlit's
   default is overflow:auto + fixed viewport height which gives the user
   the inner scrollbar they expect. An earlier attempt to force
   overflow:visible broke scrolling entirely (the parent stApp clipped
   the overflow with no scrollbar to compensate). */

[data-testid="stMainBlockContainer"] {
  max-width: 1440px !important;
  padding-top: 2rem !important;
  padding-left: 2rem !important;
  padding-right: 2rem !important;
  padding-bottom: 4rem !important;
}

/* Default text colors — wrapped in :where() so they have ZERO specificity
   and any class-targeted color rule (like .bt-hero-content *) wins. */
:where([data-testid="stMain"] h1),
:where([data-testid="stMain"] h2),
:where([data-testid="stMain"] h3),
:where([data-testid="stMain"] h4) {
  color: var(--char);
  letter-spacing: -0.025em;
  font-weight: 700;
}

:where([data-testid="stMain"] [data-testid="stMarkdownContainer"] p),
:where([data-testid="stMain"] [data-testid="stMarkdownContainer"] span),
:where([data-testid="stMain"] [data-testid="stMarkdownContainer"] div) {
  color: var(--char);
}

/* Hide streamlit chrome that fights the editorial vibe */
[data-testid="stHeader"] { background: transparent !important; }
[data-testid="stDecoration"] { display: none !important; }
.stDeployButton { display: none !important; }

/* ----------------------------------------------------------------------
   PAGE HEADER + DASHBOARD CONTROL BAR
   ---------------------------------------------------------------------- */

.bt-page-title {
  font-size: 3.5rem !important;
  font-weight: 700 !important;
  letter-spacing: -0.035em !important;
  line-height: 1 !important;
  color: var(--char) !important;
  margin: 0 !important;
}
.bt-page-subtitle {
  color: var(--bt-on-surface-variant) !important;
  font-weight: 500;
  font-size: 1rem;
  margin-top: 0.4rem;
}
.bt-refresh-eyebrow {
  font-size: 0.7rem;
  text-transform: uppercase;
  letter-spacing: 0.18em;
  color: var(--bt-outline);
  font-weight: 600;
  font-family: 'Jost', sans-serif;
}
.bt-refresh-time {
  font-weight: 600;
  color: var(--char);
  font-size: 0.95rem;
  margin-top: 2px;
}

/* Segmented control — pill-shaped radio for the category filter */
[data-testid="stRadio"] [role="radiogroup"] {
  background: var(--bt-surface-low) !important;
  border-radius: 999px !important;
  padding: 6px !important;
  box-shadow: var(--bt-shadow);
  display: inline-flex !important;
  gap: 4px;
}
[data-testid="stRadio"] [role="radiogroup"] label {
  border-radius: 999px !important;
  padding: 10px 24px !important;
  margin: 0 !important;
  cursor: pointer;
  transition: background 0.18s ease;
  background: transparent;
  display: inline-flex !important;
  align-items: center;
}
/* Inactive label text — explicit charcoal so it reads on the cream pill */
[data-testid="stRadio"] [role="radiogroup"] label,
[data-testid="stRadio"] [role="radiogroup"] label *:not(input) {
  color: var(--char) !important;
  font-weight: 600 !important;
  font-size: 0.85rem !important;
  font-family: 'Jost', 'Helvetica', sans-serif !important;
}
/* Active label — charcoal pill background with white text. Apply white
   to ALL descendants so the inner Streamlit-emitted spans inherit it. */
[data-testid="stRadio"] [role="radiogroup"] label:has(input:checked) {
  background: var(--char) !important;
}
[data-testid="stRadio"] [role="radiogroup"] label:has(input:checked),
[data-testid="stRadio"] [role="radiogroup"] label:has(input:checked) *:not(input) {
  color: #ffffff !important;
}
/* Hide the native radio dot — we want pure pill UI */
[data-testid="stRadio"] [role="radiogroup"] label > div:first-child { display: none !important; }

/* ----------------------------------------------------------------------
   HERO CARD (biggest mover) — full-bleed image with gradient overlay
   ---------------------------------------------------------------------- */

.bt-hero {
  position: relative;
  border-radius: 28px;
  overflow: hidden;
  height: 440px;
  box-shadow: var(--bt-shadow-lg);
  margin: 28px 0 40px 0;
  display: flex;
  align-items: flex-end;
  background: var(--char);
}
.bt-hero-bg {
  position: absolute;
  inset: 0;
  background-image: url('app/static/beverage-trends/hero-energy-splash.png');
  background-size: 60% auto;
  background-position: right center;
  background-repeat: no-repeat;
  filter: saturate(1.2) contrast(1.05);
  opacity: 0.95;
}
.bt-hero-overlay {
  position: absolute;
  inset: 0;
  background:
    linear-gradient(90deg,
      rgba(45, 41, 38, 0.95) 0%,
      rgba(45, 41, 38, 0.75) 40%,
      rgba(45, 41, 38, 0.15) 75%,
      rgba(45, 41, 38, 0.0) 100%),
    linear-gradient(180deg,
      rgba(45, 41, 38, 0.0) 60%,
      rgba(45, 41, 38, 0.35) 100%);
}
.bt-hero-content {
  position: relative;
  z-index: 2;
  padding: 56px;
  max-width: 880px;
}
.bt-hero-content,
.bt-hero-content * {
  color: #ffffff !important;
}
.bt-hero-eyebrow {
  display: inline-flex !important;
  align-items: center;
  gap: 6px;
  background: var(--yellow);
  color: var(--char) !important;
  padding: 7px 16px;
  border-radius: 999px;
  font-size: 0.7rem;
  font-weight: 700 !important;
  text-transform: uppercase;
  letter-spacing: 0.14em;
  margin-bottom: 22px;
}
.bt-hero-eyebrow .material-symbols-outlined { color: var(--char) !important; }
.bt-hero-title {
  font-size: 3.6rem !important;
  font-weight: 700 !important;
  letter-spacing: -0.035em !important;
  line-height: 1.04 !important;
  margin: 0 0 16px 0 !important;
  color: #ffffff !important;
  text-shadow: 0 2px 32px rgba(0, 0, 0, 0.55);
}
.bt-hero-sub {
  font-size: 1.1rem !important;
  font-weight: 500 !important;
  color: rgba(255, 255, 255, 0.92) !important;
  text-shadow: 0 1px 16px rgba(0, 0, 0, 0.55);
  margin: 0 !important;
}

/* ----------------------------------------------------------------------
   TREND CARDS (2x2 grid)
   ---------------------------------------------------------------------- */

.bt-card {
  background: var(--bt-surface-lowest);
  border-radius: 24px;
  padding: 32px 34px;
  box-shadow: var(--bt-shadow);
  margin-bottom: 0;
  border: 1px solid rgba(45, 41, 38, 0.04);
}
.bt-card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 26px;
}
.bt-card-title {
  font-size: 1.5rem !important;
  font-weight: 700 !important;
  letter-spacing: -0.02em !important;
  color: var(--char) !important;
  display: flex;
  align-items: center;
  gap: 12px;
  margin: 0 !important;
  font-family: 'Jost', sans-serif !important;
}
.bt-card-title .material-symbols-outlined {
  font-size: 28px !important;
}
.bt-card-eyebrow {
  font-size: 0.65rem;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.16em;
  color: var(--bt-outline);
}

.bt-icon-green { color: var(--forest); }
.bt-icon-yellow { color: var(--char); }
.bt-icon-pink { color: #b04d5e; }
.bt-icon-navy { color: var(--navy); }

/* Trend rows */
.bt-row {
  display: grid;
  grid-template-columns: 32px 1fr auto auto;
  gap: 16px;
  align-items: center;
  padding: 14px 8px;
  margin: 0 -8px;
  border-radius: 12px;
  transition: background 0.15s ease;
}
.bt-row + .bt-row { border-top: 1px solid rgba(45, 41, 38, 0.05); }
.bt-row:hover { background: var(--bt-surface-low); }
.bt-rank {
  font-weight: 700;
  font-size: 0.85rem;
  color: var(--bt-outline);
  font-family: 'Jost', sans-serif;
  letter-spacing: 0.02em;
}
.bt-entity {
  font-weight: 600;
  font-size: 1.05rem;
  color: var(--char);
}
.bt-type-badge {
  font-size: 0.62rem;
  padding: 3px 9px;
  border-radius: 999px;
  background: var(--bt-surface-variant);
  color: var(--bt-on-surface-variant);
  text-transform: uppercase;
  letter-spacing: 0.06em;
  font-weight: 700;
  font-family: 'Jost', sans-serif;
}
.bt-growth-pos {
  font-weight: 700;
  font-size: 0.78rem;
  background: var(--green);
  color: var(--forest);
  padding: 5px 12px;
  border-radius: 999px;
  font-family: 'Jost', sans-serif;
  letter-spacing: 0.02em;
}
.bt-growth-neg {
  font-weight: 700;
  font-size: 0.78rem;
  background: var(--pink);
  color: var(--char);
  padding: 5px 12px;
  border-radius: 999px;
  font-family: 'Jost', sans-serif;
  letter-spacing: 0.02em;
}
.bt-viral {
  font-weight: 700;
  font-size: 0.78rem;
  background: var(--yellow);
  color: var(--char);
  padding: 5px 12px;
  border-radius: 999px;
  font-family: 'Jost', sans-serif;
  letter-spacing: 0.02em;
}

/* Empty-state row */
.bt-empty-row {
  padding: 18px 8px;
  color: var(--bt-on-surface-variant);
  font-style: italic;
  font-size: 0.95rem;
}

/* ----------------------------------------------------------------------
   STAYING POWER LEADERBOARD (table format)
   ---------------------------------------------------------------------- */

.bt-section-header {
  display: flex;
  justify-content: space-between;
  align-items: flex-end;
  margin-bottom: 24px;
}
.bt-section-title {
  font-size: 2rem !important;
  font-weight: 700 !important;
  letter-spacing: -0.025em !important;
  color: var(--char) !important;
  margin: 0 !important;
  font-family: 'Jost', sans-serif !important;
}
.bt-section-legend {
  display: flex;
  gap: 18px;
}
.bt-legend-item {
  display: flex;
  align-items: center;
  gap: 6px;
  font-size: 0.7rem;
  text-transform: uppercase;
  letter-spacing: 0.1em;
  font-weight: 600;
  font-family: 'Jost', sans-serif;
  color: var(--bt-on-surface-variant);
}
.bt-legend-dot {
  width: 10px;
  height: 10px;
  border-radius: 999px;
}

.bt-sp-table {
  width: 100%;
  border-collapse: collapse;
}
.bt-sp-table thead th {
  text-align: left;
  font-size: 0.65rem;
  text-transform: uppercase;
  letter-spacing: 0.14em;
  color: var(--bt-outline);
  font-weight: 700;
  font-family: 'Jost', sans-serif;
  padding: 0 12px 14px 12px;
  border-bottom: 1px solid var(--bt-surface-variant);
}
.bt-sp-table thead th.center { text-align: center; }
.bt-sp-table tbody tr {
  transition: background 0.15s ease;
}
.bt-sp-table tbody tr:hover { background: var(--bt-surface-low); }
.bt-sp-table tbody td {
  padding: 22px 12px;
  border-bottom: 1px solid rgba(45, 41, 38, 0.04);
  vertical-align: middle;
}
.bt-sp-rank {
  font-weight: 700;
  font-size: 1rem;
  color: var(--char);
  font-family: 'Jost', sans-serif;
  width: 50px;
}
.bt-sp-entity-cell {
  display: flex;
  align-items: center;
  gap: 14px;
}
.bt-sp-entity-name {
  font-weight: 700;
  font-size: 1.1rem;
  color: var(--char);
}
.bt-sp-score {
  text-align: center;
  font-weight: 700;
  font-size: 1.5rem;
  color: var(--char);
  font-family: 'Jost', sans-serif;
  width: 110px;
}
.bt-sp-score-suffix {
  font-size: 0.7rem;
  color: var(--bt-outline);
  font-weight: 500;
}
.bt-sp-bar-wrap {
  width: 100%;
  background: var(--bt-surface-high);
  border-radius: 999px;
  height: 10px;
  overflow: hidden;
}
.bt-sp-bar-fill {
  height: 100%;
  background: linear-gradient(90deg, var(--sky), var(--navy));
  border-radius: 999px;
  transition: width 0.4s ease;
}
.bt-sp-signals {
  display: flex;
  gap: 6px;
  flex-wrap: wrap;
}
.bt-chip {
  display: inline-block;
  font-size: 0.6rem;
  padding: 4px 10px;
  border-radius: 999px;
  background: var(--bt-surface-variant);
  color: var(--bt-on-surface-variant);
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 0.06em;
  font-family: 'Jost', sans-serif;
}
.bt-chip-on {
  background: var(--green);
  color: var(--forest);
}

/* ----------------------------------------------------------------------
   DRILL-DOWN (5-card source grid)
   ---------------------------------------------------------------------- */

.bt-drill-header {
  display: flex;
  align-items: flex-end;
  justify-content: space-between;
  border-bottom: 1px solid var(--bt-outline-variant);
  padding-bottom: 16px;
  margin-bottom: 28px;
}
.bt-drill-title {
  font-size: 2rem !important;
  font-weight: 700 !important;
  letter-spacing: -0.025em !important;
  color: var(--char) !important;
  margin: 0 !important;
  font-family: 'Jost', sans-serif !important;
}
.bt-drill-title .focus {
  color: var(--navy);
  font-style: italic;
}
.bt-source-grid {
  display: grid;
  grid-template-columns: repeat(5, minmax(0, 1fr));
  gap: 20px;
}
.bt-source-card {
  background: var(--bt-surface-container);
  border-radius: 20px;
  padding: 24px;
  display: flex;
  flex-direction: column;
  justify-content: space-between;
  min-height: 280px;
  box-shadow: var(--bt-shadow);
  transition: transform 0.18s ease, box-shadow 0.18s ease;
}
.bt-source-card:hover {
  transform: translateY(-3px);
  box-shadow: var(--bt-shadow-lg);
}
.bt-engine-pill {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  background: var(--bt-surface-lowest);
  padding: 5px 12px;
  border-radius: 999px;
  font-size: 0.6rem;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 0.08em;
  color: var(--bt-on-surface-variant);
  box-shadow: 0 1px 4px rgba(45, 41, 38, 0.06);
  font-family: 'Jost', sans-serif;
  width: fit-content;
}
.bt-source-title {
  font-weight: 700;
  font-size: 1rem;
  color: var(--char);
  line-height: 1.3;
  margin-top: 14px;
}
.bt-source-snippet {
  font-size: 0.82rem;
  color: var(--bt-on-surface-variant);
  line-height: 1.5;
  margin-top: 10px;
  display: -webkit-box;
  -webkit-line-clamp: 4;
  -webkit-box-orient: vertical;
  overflow: hidden;
}
.bt-source-link {
  color: var(--navy) !important;
  font-size: 0.65rem;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 0.12em;
  margin-top: 18px;
  display: inline-flex;
  align-items: center;
  gap: 4px;
  text-decoration: none;
  font-family: 'Jost', sans-serif;
}
.bt-source-link:hover { text-decoration: underline; }

/* ----------------------------------------------------------------------
   DISCOVERY FEED (12-col grid)
   ---------------------------------------------------------------------- */

.bt-discovery-row {
  display: grid;
  grid-template-columns: 5fr 3fr 2fr 2fr;
  gap: 18px;
  align-items: center;
  padding: 18px 18px;
  border-radius: 14px;
  transition: background 0.15s ease;
}
.bt-discovery-row.alt { background: var(--bt-surface-low); }
.bt-discovery-row:hover { background: var(--bt-surface-container); }
.bt-discovery-header {
  display: grid;
  grid-template-columns: 5fr 3fr 2fr 2fr;
  gap: 18px;
  padding: 8px 18px;
  font-size: 0.6rem;
  text-transform: uppercase;
  letter-spacing: 0.14em;
  color: var(--bt-outline);
  font-weight: 700;
  font-family: 'Jost', sans-serif;
}
.bt-discovery-phrase {
  font-weight: 600;
  font-size: 1.05rem;
  color: var(--char);
}
.bt-discovery-volume {
  display: inline-block;
  padding: 5px 12px;
  background: var(--bt-surface-variant);
  color: var(--bt-on-surface-variant);
  border-radius: 999px;
  font-size: 0.65rem;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 0.06em;
  font-family: 'Jost', sans-serif;
  width: fit-content;
}
.bt-discovery-growth {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  color: var(--forest);
  font-weight: 700;
  font-size: 0.95rem;
  font-family: 'Jost', sans-serif;
}
.bt-discovery-action {
  display: inline-block;
  background: var(--char);
  color: var(--bt-surface-lowest);
  padding: 8px 16px;
  border-radius: 999px;
  font-size: 0.6rem;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 0.1em;
  font-family: 'Jost', sans-serif;
  text-align: center;
  cursor: default;
  opacity: 0.85;
}

/* ----------------------------------------------------------------------
   FOOTER
   ---------------------------------------------------------------------- */

.bt-footer {
  text-align: center;
  margin-top: 48px;
  padding: 24px;
  color: var(--bt-outline);
  font-size: 0.7rem;
  text-transform: uppercase;
  letter-spacing: 0.14em;
  font-family: 'Jost', sans-serif;
  font-weight: 600;
  border-top: 1px solid var(--bt-surface-variant);
}
</style>
"""
st.markdown(NOWADAYS_CSS, unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# BQ helpers
# ---------------------------------------------------------------------------


@st.cache_resource
def get_bq_client() -> bigquery.Client:
    try:
        if hasattr(st, "secrets") and "gcp_service_account" in st.secrets:
            from google.oauth2 import service_account

            credentials = service_account.Credentials.from_service_account_info(
                st.secrets["gcp_service_account"]
            )
            return bigquery.Client(project=BQ_PROJECT, credentials=credentials)
    except Exception:
        pass
    return bigquery.Client(project=BQ_PROJECT)


@st.cache_data(ttl=3600, show_spinner=False)
def _run(sql: str) -> pd.DataFrame:
    return get_bq_client().query(sql).to_dataframe()


def _safe(sql: str, empty_cols: list[str]) -> pd.DataFrame:
    """Run a query and return an empty DataFrame on any failure.

    We deliberately swallow BQ errors silently here and let the per-section
    empty-state UI ("No data yet — harvest needs a few cycles") handle the
    rendering. Surfacing raw BQ exception text as red banners created a wall
    of noise before the harvest had written its first row. The exception is
    still logged to stderr so it shows up in Streamlit Cloud logs for
    debugging without polluting the dashboard UI.
    """
    try:
        return _run(sql)
    except Exception as exc:  # noqa: BLE001
        import sys
        print(f"[beverage-trends] query failed: {exc}", file=sys.stderr)
        return pd.DataFrame(columns=empty_cols)


# ---------------------------------------------------------------------------
# Data loaders
# ---------------------------------------------------------------------------


def load_trending(entity_types: tuple[str, ...]) -> pd.DataFrame:
    types_list = ",".join([f"'{t}'" for t in entity_types])
    return _safe(
        f"""
        SELECT entity_name, entity_type, total_mentions, recent_7d,
               prior_avg_7d, growth_rate, avg_platforms
        FROM `{BQ_PROJECT}.{DATASET}.v_trending_28d`
        WHERE entity_type IN ({types_list})
        ORDER BY growth_rate DESC
        LIMIT 10
        """,
        ["entity_name", "entity_type", "total_mentions", "recent_7d",
         "prior_avg_7d", "growth_rate", "avg_platforms"],
    )


def load_viral(entity_types: tuple[str, ...]) -> pd.DataFrame:
    types_list = ",".join([f"'{t}'" for t in entity_types])
    return _safe(
        f"""
        SELECT entity_name, entity_type, recent_avg, baseline, spike_ratio,
               recent_platforms, recent_total
        FROM `{BQ_PROJECT}.{DATASET}.v_viral_48h`
        WHERE entity_type IN ({types_list})
        ORDER BY spike_ratio DESC
        LIMIT 10
        """,
        ["entity_name", "entity_type", "recent_avg", "baseline",
         "spike_ratio", "recent_platforms", "recent_total"],
    )


def load_declining(entity_types: tuple[str, ...]) -> pd.DataFrame:
    types_list = ",".join([f"'{t}'" for t in entity_types])
    return _safe(
        f"""
        SELECT entity_name, entity_type, total_mentions, recent_7d,
               prior_avg_7d, growth_rate
        FROM `{BQ_PROJECT}.{DATASET}.v_declining_28d`
        WHERE entity_type IN ({types_list})
        ORDER BY growth_rate ASC
        LIMIT 10
        """,
        ["entity_name", "entity_type", "total_mentions", "recent_7d",
         "prior_avg_7d", "growth_rate"],
    )


def load_staying_power() -> pd.DataFrame:
    return _safe(
        f"""
        SELECT entity_name, entity_type, days_observed, total_mentions,
               cross_platform_score, stability_score, diversity_score,
               staying_power_score
        FROM `{BQ_PROJECT}.{DATASET}.v_staying_power`
        ORDER BY staying_power_score DESC
        LIMIT 25
        """,
        ["entity_name", "entity_type", "days_observed", "total_mentions",
         "cross_platform_score", "stability_score", "diversity_score",
         "staying_power_score"],
    )


def load_sources_for_entity(entity_name: str) -> pd.DataFrame:
    safe_name = entity_name.replace("'", "''")
    return _safe(
        f"""
        SELECT source_engine, url, title, snippet, harvested_at
        FROM `{BQ_PROJECT}.{DATASET}.entity_mentions`
        WHERE entity_name = '{safe_name}'
          AND DATE(harvested_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
        ORDER BY harvested_at DESC
        LIMIT 5
        """,
        ["source_engine", "url", "title", "snippet", "harvested_at"],
    )


def load_ngram_candidates() -> pd.DataFrame:
    return _safe(
        f"""
        SELECT ngram, f_recent, f_prior, growth_ratio
        FROM `{BQ_PROJECT}.{DATASET}.v_ngram_candidates`
        LIMIT 15
        """,
        ["ngram", "f_recent", "f_prior", "growth_ratio"],
    )


def load_last_refresh() -> datetime | None:
    df = _safe(
        f"""
        SELECT MAX(harvested_at) AS last_refresh
        FROM `{BQ_PROJECT}.{DATASET}.search_results_raw`
        WHERE DATE(harvested_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
        """,
        ["last_refresh"],
    )
    if df.empty:
        return None
    val = df.iloc[0]["last_refresh"]
    # BQ returns NULL → pandas NaT when the table is empty; NaT is NOT the
    # same as None and silently survives an `is None` check, so use pd.isna.
    if pd.isna(val):
        return None
    return pd.to_datetime(val).to_pydatetime()


def load_headline_mover() -> dict | None:
    df = _safe(
        f"""
        SELECT entity_name, entity_type, growth_rate, recent_7d, avg_platforms
        FROM `{BQ_PROJECT}.{DATASET}.v_trending_28d`
        WHERE growth_rate IS NOT NULL
        ORDER BY growth_rate DESC
        LIMIT 1
        """,
        ["entity_name", "entity_type", "growth_rate", "recent_7d", "avg_platforms"],
    )
    if df.empty:
        return None
    row = df.iloc[0]
    return {
        "name": row["entity_name"],
        "type": row["entity_type"],
        "growth_pct": float(row["growth_rate"]) * 100.0,
        "mentions": int(row["recent_7d"]),
        "platforms": int(round(float(row["avg_platforms"] or 0))),
    }


# ---------------------------------------------------------------------------
# Rendering helpers
# ---------------------------------------------------------------------------


def _fmt_pct(value: float) -> str:
    return f"{value * 100:+.0f}%"


def _fmt_spike(value: float) -> str:
    return f"{value:.1f}×"


def _render_trend_card(
    title: str,
    material_icon: str,
    icon_class: str,
    eyebrow: str,
    df: pd.DataFrame,
    metric_key: str,
    metric_kind: str,
) -> None:
    """Render one of the 4 trend cards (trending / viral / brands / declining).

    `material_icon` is a Material Symbols Outlined ligature name like
    "trending_up" / "shutter_speed" / "verified" / "south_east".
    """
    if df.empty:
        rows_html = (
            "<div class='bt-empty-row'>No data yet — harvest needs a few cycles.</div>"
        )
    else:
        row_parts: list[str] = []
        for i, row in df.reset_index(drop=True).iterrows():
            chip_class = {
                "pos": "bt-growth-pos",
                "neg": "bt-growth-neg",
                "viral": "bt-viral",
            }[metric_kind]
            value = float(row[metric_key])
            if metric_kind in ("pos", "neg"):
                # Bootstrap detection: v_trending_28d's day-1 fallback returns
                # raw recent_7d as growth_rate. Show "X mentions" instead of
                # a meaningless percentage like "+106000%".
                if "recent_7d" in row.index and abs(value - float(row["recent_7d"])) < 0.5:
                    chip_value = f"{int(row['recent_7d'])} mentions"
                else:
                    chip_value = _fmt_pct(value)
            else:
                if "baseline" in row.index and float(row.get("baseline", 0) or 0) == 0:
                    recent_total = int(row.get("recent_total", value))
                    chip_value = f"{recent_total} mentions"
                else:
                    chip_value = _fmt_spike(value)
            row_parts.append(
                "<div class='bt-row'>"
                f"<div class='bt-rank'>{i + 1:02d}</div>"
                f"<div class='bt-entity'>{row['entity_name']}</div>"
                f"<div class='bt-type-badge'>{row['entity_type']}</div>"
                f"<div class='{chip_class}'>{chip_value}</div>"
                "</div>"
            )
        rows_html = "".join(row_parts)
    card_html = (
        "<div class='bt-card'>"
        "<div class='bt-card-header'>"
        f"<div class='bt-card-title'><span class='material-symbols-outlined {icon_class}'>{material_icon}</span>{title}</div>"
        f"<div class='bt-card-eyebrow'>{eyebrow}</div>"
        "</div>"
        f"{rows_html}"
        "</div>"
    )
    st.markdown(card_html, unsafe_allow_html=True)


def _render_headline(mover: dict | None) -> None:
    """Hero card with full-bleed energy-splash background image."""
    if not mover:
        title_html = "No headline mover yet — waiting for data"
        sub = "The harvest needs at least a week of runs before a headline appears."
    else:
        is_bootstrap = abs(mover["growth_pct"] - (mover["mentions"] * 100)) < 1
        if is_bootstrap:
            title_html = (
                f"{mover['name']} is leading with {mover['mentions']:,} mentions "
                f"across {mover['platforms']} platforms this week"
            )
            sub = (
                f"Category: {mover['type']} · trend baselines mature after "
                f"2 weeks of harvest history"
            )
        else:
            arrow = "up" if mover["growth_pct"] >= 0 else "down"
            title_html = (
                f"{mover['name']} is {arrow} "
                f"{abs(mover['growth_pct']):.0f}% across {mover['platforms']} platforms this week"
            )
            sub = (
                f"{mover['mentions']:,} mentions in the last 7 days · "
                f"category leader in growth velocity"
            )
    hero_html = (
        "<div class='bt-hero'>"
        "<div class='bt-hero-bg'></div>"
        "<div class='bt-hero-overlay'></div>"
        "<div class='bt-hero-content'>"
        "<div class='bt-hero-eyebrow'>"
        "<span class='material-symbols-outlined' style='font-size:14px;'>bolt</span>"
        "Biggest Mover"
        "</div>"
        f"<h2 class='bt-hero-title'>🔥 {title_html}</h2>"
        f"<p class='bt-hero-sub'>{sub}</p>"
        "</div>"
        "</div>"
    )
    st.markdown(hero_html, unsafe_allow_html=True)


def _render_staying_power(df: pd.DataFrame) -> None:
    """Editorial table layout: rank | entity | score | retention bar | signals.

    Every HTML chunk is single-line concatenation (no leading whitespace) so
    streamlit's markdown parser doesn't render it as a code block.
    """
    section_header = (
        "<div class='bt-section-header'>"
        "<h3 class='bt-section-title'>Staying Power Leaderboard</h3>"
        "<div class='bt-section-legend'>"
        "<div class='bt-legend-item'><span class='bt-legend-dot' style='background:var(--navy);'></span>Steady</div>"
        "<div class='bt-legend-item'><span class='bt-legend-dot' style='background:var(--green);'></span>Growing</div>"
        "</div>"
        "</div>"
    )

    if df.empty:
        body = (
            "<div class='bt-card'>"
            f"{section_header}"
            "<div class='bt-empty-row'>Waiting for data — staying power scores need at least one harvest cycle.</div>"
            "</div>"
        )
        st.markdown(body, unsafe_allow_html=True)
        return

    table_head = (
        "<table class='bt-sp-table'>"
        "<thead><tr>"
        "<th>Rank</th>"
        "<th>Entity</th>"
        "<th class='center'>Score</th>"
        "<th style='width:30%;'>Retention Indicator</th>"
        "<th>Signals</th>"
        "</tr></thead>"
        "<tbody>"
    )

    body_parts: list[str] = [table_head]
    for i, row in df.reset_index(drop=True).iterrows():
        score = int(row["staying_power_score"] or 0)
        signals = [
            ("Cross-Platform", float(row["cross_platform_score"] or 0) >= 0.5),
            ("Stable", float(row["stability_score"] or 0) >= 0.5),
            ("Broad Sources", float(row["diversity_score"] or 0) >= 0.5),
        ]
        signals_html = "".join(
            f"<span class='bt-chip {'bt-chip-on' if on else ''}'>{label}</span>"
            for label, on in signals
        )
        body_parts.append(
            "<tr>"
            f"<td class='bt-sp-rank'>{i + 1:02d}</td>"
            "<td>"
            "<div class='bt-sp-entity-cell'>"
            f"<span class='bt-sp-entity-name'>{row['entity_name']}</span>"
            f"<span class='bt-type-badge'>{row['entity_type']}</span>"
            "</div>"
            "</td>"
            f"<td class='bt-sp-score'>{score}<span class='bt-sp-score-suffix'>/100</span></td>"
            "<td>"
            "<div class='bt-sp-bar-wrap'>"
            f"<div class='bt-sp-bar-fill' style='width:{score}%;'></div>"
            "</div>"
            "</td>"
            f"<td><div class='bt-sp-signals'>{signals_html}</div></td>"
            "</tr>"
        )
    body_parts.append("</tbody></table>")

    card_html = (
        "<div class='bt-card'>"
        f"{section_header}"
        f"{''.join(body_parts)}"
        "</div>"
    )
    st.markdown(card_html, unsafe_allow_html=True)


_ENGINE_ICON = {
    "google": "language",
    "bing": "language",
    "duckduckgo": "language",
    "reddit": "forum",
    "youtube": "play_circle",
    "youtube_noapi": "play_circle",
    "tiktok": "movie",
    "twitter": "share",
    "x": "share",
    "instagram": "photo_camera",
    "pinterest": "push_pin",
    "facebook": "thumb_up",
}


def _engine_icon(engine: str) -> str:
    return _ENGINE_ICON.get((engine or "").lower(), "public")


def _render_drilldown(df_trending: pd.DataFrame) -> None:
    if df_trending.empty:
        st.markdown(
            "<div class='bt-card'><div class='bt-section-header'>"
            "<h3 class='bt-section-title'>Drill into a trend</h3>"
            "</div><div class='bt-empty-row'>"
            "Drill-down will activate once trending data is available."
            "</div></div>",
            unsafe_allow_html=True,
        )
        return

    options = df_trending["entity_name"].tolist()
    # Render the section heading + selector via Streamlit so the dropdown works
    st.markdown(
        "<div class='bt-drill-header'>"
        "<h3 class='bt-drill-title'>Deep Dive</h3>"
        "</div>",
        unsafe_allow_html=True,
    )
    selected = st.selectbox(
        "Pick an entity to see source content",
        options,
        label_visibility="collapsed",
    )

    sources = load_sources_for_entity(selected)
    if sources.empty:
        st.markdown(
            f"<div class='bt-empty-row'>No recent source content for {selected} yet.</div>",
            unsafe_allow_html=True,
        )
        return

    cards: list[str] = []
    for _, row in sources.iterrows():
        title = (row["title"] or "(no title)").replace("<", "&lt;").replace(">", "&gt;")
        snippet = ((row["snippet"] or "")[:240]).replace("<", "&lt;").replace(">", "&gt;")
        engine = (row["source_engine"] or "web")
        engine_label = engine.replace("_noapi", "").replace("_", " ").title()
        url = row["url"]
        cards.append(
            "<div class='bt-source-card'>"
            "<div>"
            f"<span class='bt-engine-pill'>"
            f"<span class='material-symbols-outlined' style='font-size:12px;'>{_engine_icon(engine)}</span>"
            f"{engine_label}"
            "</span>"
            f"<div class='bt-source-title'>{title}</div>"
            f"<div class='bt-source-snippet'>{snippet}…</div>"
            "</div>"
            f"<a class='bt-source-link' href='{url}' target='_blank' rel='noopener'>"
            "View Context "
            "<span class='material-symbols-outlined' style='font-size:14px;'>arrow_forward</span>"
            "</a>"
            "</div>"
        )
    grid_html = (
        f"<div class='bt-source-grid'>{''.join(cards)}</div>"
    )
    st.markdown(grid_html, unsafe_allow_html=True)


def _render_discovery(df: pd.DataFrame) -> None:
    section_header = (
        "<div class='bt-section-header'>"
        "<h3 class='bt-section-title'>Emerging Dialects &amp; Discovery</h3>"
        "</div>"
    )
    if df.empty:
        body = (
            "<div class='bt-card'>"
            f"{section_header}"
            "<div class='bt-empty-row'>No n-gram candidates yet.</div>"
            "</div>"
        )
        st.markdown(body, unsafe_allow_html=True)
        return

    header_row = (
        "<div class='bt-discovery-header'>"
        "<div>Phrase</div>"
        "<div>Volume this week</div>"
        "<div>Growth Ratio</div>"
        "<div style='text-align:right;'>Action</div>"
        "</div>"
    )
    row_parts: list[str] = [header_row]
    for i, row in df.reset_index(drop=True).iterrows():
        ratio = float(row["growth_ratio"] or 0)
        f_recent = int(row["f_recent"])
        alt = " alt" if i % 2 == 0 else ""
        row_parts.append(
            f"<div class='bt-discovery-row{alt}'>"
            f"<div class='bt-discovery-phrase'>&ldquo;{row['ngram']}&rdquo;</div>"
            f"<div><span class='bt-discovery-volume'>{f_recent:,} mentions</span></div>"
            "<div>"
            "<div class='bt-discovery-growth'>"
            "<span class='material-symbols-outlined' style='font-size:18px;'>trending_up</span>"
            f"{ratio:.1f}×"
            "</div>"
            "</div>"
            "<div style='text-align:right;'>"
            "<span class='bt-discovery-action'>Add to dictionary</span>"
            "</div>"
            "</div>"
        )
    rows_html = "".join(row_parts)
    card_html = (
        "<div class='bt-card'>"
        f"{section_header}"
        "<div style='font-size:0.82rem; color:var(--bt-on-surface-variant); margin-bottom:18px;'>"
        "Bigrams and trigrams that spiked this week but aren't yet in the dictionary. "
        "Review weekly and promote real entities into <code>entities.yaml</code>."
        "</div>"
        f"{rows_html}"
        "</div>"
    )
    st.markdown(card_html, unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Main render
# ---------------------------------------------------------------------------


def main() -> None:
    # ----- Editorial header: title + tagline on left, refresh meta on right
    col_head, col_refresh = st.columns([5, 2])
    with col_head:
        st.markdown(
            "<h1 class='bt-page-title'>🥤 Beverage Trends</h1>"
            "<p class='bt-page-subtitle'>Market intelligence for the modern beverage landscape.</p>",
            unsafe_allow_html=True,
        )
    with col_refresh:
        last_refresh = load_last_refresh()
        refresh_txt = (
            last_refresh.strftime("%b %d, %H:%M UTC")
            if last_refresh
            else "No harvest yet"
        )
        st.markdown(
            "<div style='text-align:right; padding-top:14px;'>"
            "<div class='bt-refresh-eyebrow'>Last Refreshed</div>"
            f"<div class='bt-refresh-time'>{refresh_txt}</div>"
            "</div>",
            unsafe_allow_html=True,
        )

    # ----- Category segmented control (own row, full-width)
    category = st.radio(
        "Category",
        ["All beverages", "Functional", "THC / intoxicating"],
        horizontal=True,
        label_visibility="collapsed",
    )
    # The category toggle is a lightweight affordance; views don't slice by
    # category yet (drives off entities.category which isn't joined into
    # trend views — future work). Kept so Lisa sees the control.
    _ = category

    # ----- Hero card (biggest mover, full-bleed image)
    _render_headline(load_headline_mover())

    # ----- 2x2 trend grid
    trending_flavors = load_trending(("flavor", "ingredient"))
    viral = load_viral(("brand", "ingredient", "flavor", "category"))
    trending_brands = load_trending(("brand",))
    declining = load_declining(("brand", "ingredient", "flavor", "category"))

    col1, col2 = st.columns(2, gap="large")
    with col1:
        _render_trend_card(
            "Trending Flavors",
            "trending_up",
            "bt-icon-green",
            "Global Data",
            trending_flavors,
            "growth_rate",
            "pos",
        )
    with col2:
        _render_trend_card(
            "Viral This Week",
            "shutter_speed",
            "bt-icon-yellow",
            "Velocity Spike",
            viral,
            "spike_ratio",
            "viral",
        )

    col3, col4 = st.columns(2, gap="large")
    with col3:
        _render_trend_card(
            "Trending Brands",
            "verified",
            "bt-icon-navy",
            "Market Share",
            trending_brands,
            "growth_rate",
            "pos",
        )
    with col4:
        _render_trend_card(
            "Losing Traction",
            "south_east",
            "bt-icon-pink",
            "Cooling Down",
            declining,
            "growth_rate",
            "neg",
        )

    # ----- Staying power (table)
    _render_staying_power(load_staying_power())

    # ----- Drill-down (5-card grid; selector picks the entity)
    combined = pd.concat(
        [trending_flavors, trending_brands], ignore_index=True
    ).drop_duplicates(subset=["entity_name"])
    _render_drilldown(combined)

    # ----- Discovery feed
    _render_discovery(load_ngram_candidates())

    # ----- Footer
    st.markdown(
        "<div class='bt-footer'>"
        "Data harvested 3×/day via self-hosted SearXNG · zero-cost pipeline"
        "</div>",
        unsafe_allow_html=True,
    )


main()
