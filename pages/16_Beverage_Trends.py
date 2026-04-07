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
@import url('https://fonts.googleapis.com/css2?family=Jost:wght@400;500;600;700&display=swap');

:root {
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
}

/* Apply Jost globally on this page — Streamlit's modern DOM uses
   stAppViewContainer / stMain / stMainBlockContainer, not the old
   section.main + .block-container hierarchy. Targeting the stApp
   wrapper catches both the container and every nested emotion class
   without relying on the volatile class hashes. */
[data-testid="stAppViewContainer"],
[data-testid="stAppViewContainer"] *,
[data-testid="stMain"],
[data-testid="stMain"] *,
[data-testid="stMarkdownContainer"],
[data-testid="stMarkdownContainer"] * {
  font-family: 'Jost', 'Helvetica', sans-serif !important;
}

[data-testid="stApp"] {
  background: linear-gradient(135deg, var(--mist) 0%, var(--white) 60%, var(--cream) 200%) !important;
}

[data-testid="stMainBlockContainer"] {
  max-width: 100% !important;
  padding-top: 1.5rem !important;
  padding-left: 2rem !important;
  padding-right: 2rem !important;
}

[data-testid="stMain"] h1,
[data-testid="stMain"] h2,
[data-testid="stMain"] h3,
[data-testid="stMain"] h4 {
  color: var(--char) !important;
  letter-spacing: -0.01em;
}

[data-testid="stMain"] [data-testid="stMarkdownContainer"] p,
[data-testid="stMain"] [data-testid="stMarkdownContainer"] span,
[data-testid="stMain"] [data-testid="stMarkdownContainer"] div {
  color: var(--char);
}

/* Headline card */
.bt-headline {
  background: linear-gradient(135deg, var(--cream) 0%, var(--yellow) 100%);
  border-radius: 24px;
  padding: 36px 40px;
  color: var(--char);
  box-shadow: 0 12px 36px rgba(45, 41, 38, 0.12);
  margin: 8px 0 28px 0;
}
.bt-headline .eyebrow {
  text-transform: uppercase;
  letter-spacing: 0.12em;
  font-size: 0.75rem;
  font-weight: 600;
  opacity: 0.7;
  margin-bottom: 8px;
}
.bt-headline .title {
  font-size: clamp(1.8rem, 3.2vw, 2.6rem);
  font-weight: 700;
  line-height: 1.15;
  margin: 0;
}
.bt-headline .sub {
  margin-top: 10px;
  font-size: 1rem;
  opacity: 0.8;
}

/* Tile card */
.bt-card {
  background: var(--white);
  border-radius: 20px;
  padding: 24px 26px;
  box-shadow: 0 6px 24px rgba(45, 41, 38, 0.08);
  margin-bottom: 22px;
  border: 1px solid rgba(45, 41, 38, 0.04);
}
.bt-card h3 {
  font-size: 1.1rem;
  font-weight: 700;
  margin: 0 0 18px 0 !important;
  color: var(--char) !important;
  display: flex;
  align-items: center;
  gap: 10px;
}
.bt-card .tile-icon {
  width: 30px;
  height: 30px;
  border-radius: 8px;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  font-size: 1rem;
}
.bt-icon-green { background: var(--green); }
.bt-icon-yellow { background: var(--yellow); }
.bt-icon-pink { background: var(--pink); }
.bt-icon-navy { background: var(--navy); color: var(--white) !important; }

/* Row items */
.bt-row {
  display: grid;
  grid-template-columns: 28px 1fr auto auto;
  gap: 12px;
  align-items: center;
  padding: 10px 0;
  border-bottom: 1px solid rgba(45, 41, 38, 0.06);
}
.bt-row:last-child { border-bottom: none; }
.bt-rank {
  font-weight: 700;
  font-size: 0.9rem;
  color: var(--navy);
}
.bt-entity {
  font-weight: 500;
  font-size: 1rem;
  color: var(--char);
}
.bt-type-badge {
  font-size: 0.7rem;
  padding: 2px 8px;
  border-radius: 999px;
  background: var(--mist);
  color: var(--char);
  text-transform: uppercase;
  letter-spacing: 0.04em;
}
.bt-growth-pos {
  font-weight: 600;
  font-size: 0.9rem;
  background: var(--green);
  color: var(--forest);
  padding: 4px 10px;
  border-radius: 999px;
}
.bt-growth-neg {
  font-weight: 600;
  font-size: 0.9rem;
  background: var(--pink);
  color: var(--char);
  padding: 4px 10px;
  border-radius: 999px;
}
.bt-viral {
  font-weight: 600;
  font-size: 0.9rem;
  background: var(--yellow);
  color: var(--char);
  padding: 4px 10px;
  border-radius: 999px;
}

/* Staying-power bar */
.bt-sp-bar-wrap {
  width: 100%;
  background: var(--mist);
  border-radius: 999px;
  height: 10px;
  overflow: hidden;
}
.bt-sp-bar-fill {
  height: 100%;
  background: linear-gradient(90deg, var(--sky), var(--navy));
  border-radius: 999px;
}

.bt-chip {
  display: inline-block;
  font-size: 0.68rem;
  padding: 2px 8px;
  border-radius: 999px;
  margin-right: 4px;
  background: var(--mist);
  color: var(--char);
}
.bt-chip-on {
  background: var(--forest);
  color: var(--white);
}

/* Source link list */
.bt-source {
  padding: 8px 0;
  border-bottom: 1px solid rgba(45, 41, 38, 0.06);
  font-size: 0.88rem;
}
.bt-source a {
  color: var(--navy);
  text-decoration: none;
  font-weight: 500;
}
.bt-source a:hover { text-decoration: underline; }
.bt-source .engine-pill {
  display: inline-block;
  font-size: 0.65rem;
  padding: 2px 7px;
  border-radius: 999px;
  background: var(--navy);
  color: var(--white);
  margin-right: 8px;
  text-transform: uppercase;
  letter-spacing: 0.04em;
}

/* Discovery feed rows */
.bt-ngram-row {
  display: grid;
  grid-template-columns: 1fr auto auto;
  gap: 12px;
  align-items: center;
  padding: 10px 0;
  border-bottom: 1px solid rgba(45, 41, 38, 0.06);
}
.bt-ngram-text { font-weight: 500; }
.bt-ngram-growth {
  background: var(--yellow);
  color: var(--char);
  padding: 2px 10px;
  border-radius: 999px;
  font-size: 0.8rem;
  font-weight: 600;
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
               recent_platforms
        FROM `{BQ_PROJECT}.{DATASET}.v_viral_48h`
        WHERE entity_type IN ({types_list})
        ORDER BY spike_ratio DESC
        LIMIT 10
        """,
        ["entity_name", "entity_type", "recent_avg", "baseline",
         "spike_ratio", "recent_platforms"],
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
    title: str, icon: str, icon_class: str, df: pd.DataFrame, metric_key: str, metric_kind: str
) -> None:
    rows_html = ""
    if df.empty:
        rows_html = (
            "<div class='bt-row'><div></div>"
            "<div class='bt-entity'>No data yet — harvest needs a few cycles.</div>"
            "<div></div><div></div></div>"
        )
    else:
        for i, row in df.reset_index(drop=True).iterrows():
            chip_class = {
                "pos": "bt-growth-pos",
                "neg": "bt-growth-neg",
                "viral": "bt-viral",
            }[metric_kind]
            if metric_kind in ("pos", "neg"):
                chip_value = _fmt_pct(float(row[metric_key]))
            else:
                chip_value = _fmt_spike(float(row[metric_key]))
            rows_html += (
                "<div class='bt-row'>"
                f"<div class='bt-rank'>#{i + 1}</div>"
                f"<div class='bt-entity'>{row['entity_name']}</div>"
                f"<div class='bt-type-badge'>{row['entity_type']}</div>"
                f"<div class='{chip_class}'>{chip_value}</div>"
                "</div>"
            )
    st.markdown(
        f"""
        <div class='bt-card'>
          <h3><span class='tile-icon {icon_class}'>{icon}</span>{title}</h3>
          {rows_html}
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_headline(mover: dict | None) -> None:
    if not mover:
        title_html = "No headline mover yet — waiting for data."
        sub = "The harvest needs at least a week of runs before a headline appears."
    else:
        arrow = "up" if mover["growth_pct"] >= 0 else "down"
        title_html = (
            f"{mover['name']} is {arrow} "
            f"{abs(mover['growth_pct']):.0f}% across {mover['platforms']} platforms this week"
        )
        sub = (
            f"{mover['mentions']} mentions in the last 7 days · "
            f"type: {mover['type']}"
        )
    st.markdown(
        f"""
        <div class='bt-headline'>
          <div class='eyebrow'>This week's biggest mover</div>
          <div class='title'>🔥 {title_html}</div>
          <div class='sub'>{sub}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_staying_power(df: pd.DataFrame) -> None:
    if df.empty:
        body = "<div class='bt-row'><div></div><div class='bt-entity'>Waiting for data.</div><div></div><div></div></div>"
    else:
        body = ""
        for i, row in df.reset_index(drop=True).iterrows():
            score = int(row["staying_power_score"] or 0)
            chips_html = ""
            chips = [
                ("🌐 cross-platform", float(row["cross_platform_score"] or 0) >= 0.5),
                ("📈 stable", float(row["stability_score"] or 0) >= 0.5),
                ("🏷 broad sources", float(row["diversity_score"] or 0) >= 0.5),
            ]
            for label, on in chips:
                chips_html += (
                    f"<span class='bt-chip {'bt-chip-on' if on else ''}'>{label}</span>"
                )
            body += f"""
            <div style='padding: 14px 0; border-bottom: 1px solid rgba(45,41,38,0.06);'>
              <div style='display:flex; justify-content:space-between; align-items:center; margin-bottom:8px;'>
                <div>
                  <span class='bt-rank'>#{i + 1}</span>
                  <span class='bt-entity' style='margin-left:10px;'>{row['entity_name']}</span>
                  <span class='bt-type-badge' style='margin-left:8px;'>{row['entity_type']}</span>
                </div>
                <div style='font-weight:700; color:var(--navy); font-size:1.1rem;'>{score}/100</div>
              </div>
              <div class='bt-sp-bar-wrap'>
                <div class='bt-sp-bar-fill' style='width:{score}%;'></div>
              </div>
              <div style='margin-top:8px;'>{chips_html}</div>
            </div>
            """
    st.markdown(
        f"""
        <div class='bt-card'>
          <h3><span class='tile-icon bt-icon-navy'>⏳</span>Staying Power Leaderboard</h3>
          {body}
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_drilldown(df_trending: pd.DataFrame) -> None:
    st.markdown("### 🔍 Drill into a trend")
    if df_trending.empty:
        st.info("Drill-down will activate once trending data is available.")
        return
    options = df_trending["entity_name"].tolist()
    selected = st.selectbox("Pick an entity to see source content:", options)
    sources = load_sources_for_entity(selected)
    if sources.empty:
        st.info(f"No recent source content for {selected} yet.")
        return
    for _, row in sources.iterrows():
        title = row["title"] or "(no title)"
        snippet = (row["snippet"] or "")[:220]
        engine = row["source_engine"] or "web"
        url = row["url"]
        st.markdown(
            f"""
            <div class='bt-source'>
              <span class='engine-pill'>{engine}</span>
              <a href='{url}' target='_blank'>{title}</a>
              <div style='color:#555; margin-top:4px;'>{snippet}…</div>
            </div>
            """,
            unsafe_allow_html=True,
        )


def _render_discovery(df: pd.DataFrame) -> None:
    if df.empty:
        body = "<div class='bt-ngram-row'><div class='bt-ngram-text'>No n-gram candidates yet.</div><div></div><div></div></div>"
    else:
        body = ""
        for _, row in df.iterrows():
            ratio = float(row["growth_ratio"] or 0)
            body += (
                "<div class='bt-ngram-row'>"
                f"<div class='bt-ngram-text'>{row['ngram']}</div>"
                f"<div class='bt-chip'>{int(row['f_recent'])} this week</div>"
                f"<div class='bt-ngram-growth'>{ratio:.1f}× growth</div>"
                "</div>"
            )
    st.markdown(
        f"""
        <div class='bt-card'>
          <h3><span class='tile-icon bt-icon-yellow'>🧪</span>Discovery feed — emerging phrases</h3>
          <div style='font-size:0.85rem; color:#555; margin-bottom:12px;'>
            Bigrams and trigrams that spiked this week but aren't yet in the
            dictionary. Review weekly and promote any real entities into
            <code>scripts/beverage-trends/entities.yaml</code>.
          </div>
          {body}
        </div>
        """,
        unsafe_allow_html=True,
    )


# ---------------------------------------------------------------------------
# Main render
# ---------------------------------------------------------------------------


def main() -> None:
    # Header + refresh (two columns; category toggle gets its own row below so
    # it doesn't get crushed next to the title on narrower viewports).
    col_head, col_refresh = st.columns([4, 2])
    with col_head:
        st.markdown(
            "<h1 style='margin:0; font-size:2.4rem; font-weight:700;'>🥤 Beverage Trends</h1>"
            "<div style='color:#555; margin-top:4px;'>Functional + THC beverage signal harvested from web + social</div>",
            unsafe_allow_html=True,
        )
    with col_refresh:
        last_refresh = load_last_refresh()
        refresh_txt = (
            last_refresh.strftime("%b %d, %I:%M %p UTC")
            if last_refresh
            else "No harvest yet"
        )
        st.markdown(
            f"<div style='text-align:right; padding-top:12px;'>"
            f"<div style='font-size:0.72rem; text-transform:uppercase; letter-spacing:0.08em; opacity:0.6;'>Last refresh</div>"
            f"<div style='font-weight:600; color:var(--char);'>{refresh_txt}</div>"
            f"</div>",
            unsafe_allow_html=True,
        )

    # Category toggle on its own row — full width, no cramping.
    category = st.radio(
        "Category",
        ["All beverages", "Functional", "THC / intoxicating"],
        horizontal=True,
        label_visibility="collapsed",
    )

    # The category toggle is a lightweight hint for future filtering; views
    # don't slice by category yet (it's driven from the entities.category
    # column which isn't joined into the trend views — future work). For now
    # we render it so Lisa sees the affordance.
    _ = category  # keep linter happy until category filtering lands

    # Headline card
    _render_headline(load_headline_mover())

    # 4-tile grid
    col1, col2 = st.columns(2)
    col3, col4 = st.columns(2)

    trending_flavors = load_trending(("flavor", "ingredient"))
    viral = load_viral(("brand", "ingredient", "flavor", "category"))
    trending_brands = load_trending(("brand",))
    declining = load_declining(("brand", "ingredient", "flavor", "category"))

    with col1:
        _render_trend_card(
            "Trending Flavors & Ingredients",
            "📈",
            "bt-icon-green",
            trending_flavors,
            "growth_rate",
            "pos",
        )
    with col2:
        _render_trend_card(
            "Viral This Week",
            "🔥",
            "bt-icon-yellow",
            viral,
            "spike_ratio",
            "viral",
        )
    with col3:
        _render_trend_card(
            "Trending Brands",
            "🏷",
            "bt-icon-green",
            trending_brands,
            "growth_rate",
            "pos",
        )
    with col4:
        _render_trend_card(
            "Losing Traction",
            "📉",
            "bt-icon-pink",
            declining,
            "growth_rate",
            "neg",
        )

    # Staying power
    _render_staying_power(load_staying_power())

    # Drill-down (uses union of trending sets for the picker)
    combined = pd.concat(
        [trending_flavors, trending_brands], ignore_index=True
    ).drop_duplicates(subset=["entity_name"])
    _render_drilldown(combined)

    # Discovery feed
    _render_discovery(load_ngram_candidates())

    st.markdown(
        "<div style='text-align:center; margin-top:24px; padding:12px; color:#888; font-size:0.8rem;'>"
        "Data harvested 3×/day via self-hosted SearXNG · spec: docs/superpowers/specs/2026-04-07-beverage-trends-dashboard-design.md"
        "</div>",
        unsafe_allow_html=True,
    )


main()
