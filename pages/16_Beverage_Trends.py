"""
Beverage Trends Dashboard
Social-media + web mention tracker for functional and THC/intoxicating beverages.
Data harvested 3x/day by scripts/beverage-trends/harvest.py via SearXNG.
Designed per spec docs/superpowers/specs/2026-04-07-beverage-trends-dashboard-design.md

This page is the canonical reference implementation of the Nowadays editorial
style. All visuals come from `nowadays_ui` — see STYLE_GUIDE.md for the full
component vocabulary.
"""

from __future__ import annotations

from datetime import datetime

import pandas as pd
import streamlit as st
from google.cloud import bigquery

from nowadays_ui import (
    chip,
    growth_chip,
    inject_editorial_style,
    progress_bar,
    render_card,
    render_empty_row,
    render_footer,
    render_full_section,
    render_hero,
    render_page_header,
    render_row,
    render_section_header,
    type_badge,
)
from kpi_guard import KpiCheck, validate_kpis

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Beverage Trends",
    page_icon="🥤",
    layout="wide",
    initial_sidebar_state="collapsed",
)

inject_editorial_style()

BQ_PROJECT = "artful-logic-475116-p1"
DATASET = "beverage_trends"


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

    BQ errors are logged to stderr (visible in Streamlit Cloud logs) but
    NOT surfaced as red banners on the dashboard — the per-section empty
    states handle that gracefully.
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
        ORDER BY staying_power_score DESC, total_mentions DESC
        LIMIT 10
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
        LIMIT 10
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
# Page-specific render helpers (compose nowadays_ui primitives)
# ---------------------------------------------------------------------------


def _fmt_pct(value: float) -> str:
    return f"{value * 100:+.0f}%"


def _fmt_spike(value: float) -> str:
    return f"{value:.1f}×"


def _trend_card_body(df: pd.DataFrame, metric_key: str, metric_kind: str) -> str:
    """Build the rows HTML for a trend card. Detects the day-1 bootstrap
    case where the view's growth_rate falls back to raw mention counts and
    swaps the chip text for "X mentions" instead of an absurd percentage.
    """
    if df.empty:
        return render_empty_row("No data yet — harvest needs a few cycles.")
    parts: list[str] = []
    for i, row in df.reset_index(drop=True).iterrows():
        value = float(row[metric_key])
        if metric_kind in ("pos", "neg"):
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
        parts.append(
            render_row(
                rank=i + 1,
                entity=row["entity_name"],
                badge=row["entity_type"],
                chip_html=growth_chip(chip_value, kind=metric_kind),
            )
        )
    return "".join(parts)


def render_headline_hero(mover: dict | None) -> None:
    """Render the biggest-mover hero card with the energy splash background.

    Detects the bootstrap-week case (where growth_rate is actually raw
    recent mention count) and uses leading-with-N copy instead of a
    nonsense percentage.
    """
    if not mover:
        render_hero(
            title="No headline mover yet — waiting for data",
            subtitle="The harvest needs at least a week of runs before a headline appears.",
            eyebrow="Biggest Mover",
            eyebrow_icon="bolt",
            image_path="beverage-trends/hero-energy-splash.png",
        )
        return

    is_bootstrap = abs(mover["growth_pct"] - (mover["mentions"] * 100)) < 1
    if is_bootstrap:
        title = (
            f"🔥 {mover['name']} is leading with {mover['mentions']:,} mentions "
            f"across {mover['platforms']} platforms this week"
        )
        subtitle = (
            f"Category: {mover['type']} · trend baselines mature after "
            "2 weeks of harvest history"
        )
    else:
        arrow = "up" if mover["growth_pct"] >= 0 else "down"
        title = (
            f"🔥 {mover['name']} is {arrow} {abs(mover['growth_pct']):.0f}% "
            f"across {mover['platforms']} platforms this week"
        )
        subtitle = (
            f"{mover['mentions']:,} mentions in the last 7 days · "
            "category leader in growth velocity"
        )

    render_hero(
        title=title,
        subtitle=subtitle,
        eyebrow="Biggest Mover",
        eyebrow_icon="bolt",
        image_path="beverage-trends/hero-energy-splash.png",
    )


def render_staying_power_section(df: pd.DataFrame) -> None:
    """Editorial table layout: rank | entity | score | retention bar | signals."""
    legend = [("Steady", "var(--nw-navy)"), ("Growing", "var(--nw-green)")]
    section_header = render_section_header("Staying Power Leaderboard", legend)

    if df.empty:
        body = (
            f"<div class='nw-card'>{section_header}"
            f"{render_empty_row('Waiting for data — staying power needs at least one harvest cycle.')}"
            "</div>"
        )
        st.markdown(body, unsafe_allow_html=True)
        return

    rows: list[str] = [
        "<table class='nw-table'>"
        "<thead><tr>"
        "<th>Rank</th>"
        "<th>Entity</th>"
        "<th class='center'>Score</th>"
        "<th style='width:30%;'>Retention Indicator</th>"
        "<th>Signals</th>"
        "</tr></thead>"
        "<tbody>"
    ]
    for i, row in df.reset_index(drop=True).iterrows():
        score = int(row["staying_power_score"] or 0)
        signals = [
            ("Cross-Platform", float(row["cross_platform_score"] or 0) >= 0.5),
            ("Stable", float(row["stability_score"] or 0) >= 0.5),
            ("Broad Sources", float(row["diversity_score"] or 0) >= 0.5),
        ]
        signals_html = "".join(chip(label, active=on) for label, on in signals)
        rows.append(
            "<tr>"
            f"<td class='nw-table-rank'>{i + 1:02d}</td>"
            "<td>"
            "<div class='nw-table-entity-cell'>"
            f"<span class='nw-table-entity-name'>{row['entity_name']}</span>"
            f"{type_badge(row['entity_type'])}"
            "</div>"
            "</td>"
            f"<td class='nw-table-score'>{score}<span class='nw-table-score-suffix'>/100</span></td>"
            f"<td>{progress_bar(score)}</td>"
            f"<td>{signals_html}</td>"
            "</tr>"
        )
    rows.append("</tbody></table>")

    card_html = (
        f"<div class='nw-card'>{section_header}{''.join(rows)}</div>"
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


def render_drilldown_section(df_trending: pd.DataFrame) -> None:
    """5-card source grid for the selected entity."""
    if df_trending.empty:
        render_full_section(
            "Drill into a trend",
            render_empty_row("Drill-down will activate once trending data is available."),
        )
        return

    options = df_trending["entity_name"].tolist()
    st.markdown(
        render_section_header("Deep Dive"),
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
            render_empty_row(f"No recent source content for {selected} yet."),
            unsafe_allow_html=True,
        )
        return

    cards: list[str] = []
    for _, row in sources.iterrows():
        title = (row["title"] or "(no title)").replace("<", "&lt;").replace(">", "&gt;")
        snippet = ((row["snippet"] or "")[:240]).replace("<", "&lt;").replace(">", "&gt;")
        engine = row["source_engine"] or "web"
        engine_label = engine.replace("_noapi", "").replace("_", " ").title()
        url = row["url"]
        cards.append(
            "<div class='nw-source-card'>"
            "<div>"
            f"<span class='nw-source-pill'>"
            f"<span class='material-symbols-outlined' style='font-size:12px;'>{_engine_icon(engine)}</span>"
            f"{engine_label}"
            "</span>"
            f"<div class='nw-source-title'>{title}</div>"
            f"<div class='nw-source-snippet'>{snippet}…</div>"
            "</div>"
            f"<a class='nw-source-link' href='{url}' target='_blank' rel='noopener'>"
            "View Context "
            "<span class='material-symbols-outlined' style='font-size:14px;'>arrow_forward</span>"
            "</a>"
            "</div>"
        )
    st.markdown(
        f"<div class='nw-source-grid'>{''.join(cards)}</div>",
        unsafe_allow_html=True,
    )


def render_discovery_section(df: pd.DataFrame) -> None:
    """Discovery feed: emerging n-grams not yet in the dictionary."""
    section_header = render_section_header("Emerging Dialects & Discovery")

    if df.empty:
        body = (
            f"<div class='nw-card'>{section_header}"
            f"{render_empty_row('No n-gram candidates yet.')}"
            "</div>"
        )
        st.markdown(body, unsafe_allow_html=True)
        return

    header_row = (
        "<div class='nw-discovery-header'>"
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
            f"<div class='nw-discovery-row{alt}'>"
            f"<div class='nw-discovery-phrase'>&ldquo;{row['ngram']}&rdquo;</div>"
            f"<div><span class='nw-discovery-volume'>{f_recent:,} mentions</span></div>"
            "<div>"
            "<div class='nw-discovery-growth'>"
            "<span class='material-symbols-outlined' style='font-size:18px;'>trending_up</span>"
            f"{ratio:.1f}×"
            "</div>"
            "</div>"
            "<div style='text-align:right;'>"
            "<span class='nw-discovery-action'>Add to dictionary</span>"
            "</div>"
            "</div>"
        )
    intro = (
        "<div style='font-size:0.82rem; color:var(--nw-on-surface-variant); margin-bottom:18px;'>"
        "Bigrams and trigrams that spiked this week but aren't yet in the dictionary. "
        "Review weekly and promote real entities into <code>entities.yaml</code>."
        "</div>"
    )
    card_html = (
        f"<div class='nw-card'>{section_header}{intro}{''.join(row_parts)}</div>"
    )
    st.markdown(card_html, unsafe_allow_html=True)


# Discovery feed-specific CSS — page-local since no other dashboard uses
# this layout yet. If a second dashboard needs it, promote into nowadays_ui.
st.markdown(
    """
<style>
.nw-discovery-row {
  display: grid;
  grid-template-columns: 5fr 3fr 2fr 2fr;
  gap: 18px;
  align-items: center;
  padding: 18px 18px;
  border-radius: 14px;
  transition: background 0.15s ease;
}
.nw-discovery-row.alt { background: var(--nw-surface-low); }
.nw-discovery-row:hover { background: var(--nw-surface-container); }
.nw-discovery-header {
  display: grid;
  grid-template-columns: 5fr 3fr 2fr 2fr;
  gap: 18px;
  padding: 8px 18px;
  font-size: 0.6rem;
  text-transform: uppercase;
  letter-spacing: 0.14em;
  color: var(--nw-outline);
  font-weight: 700;
}
.nw-discovery-phrase {
  font-weight: 600;
  font-size: 1.05rem;
  color: var(--nw-char);
}
.nw-discovery-volume {
  display: inline-block;
  padding: 5px 12px;
  background: var(--nw-surface-variant);
  color: var(--nw-on-surface-variant);
  border-radius: 999px;
  font-size: 0.65rem;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 0.06em;
}
.nw-discovery-growth {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  color: var(--nw-forest);
  font-weight: 700;
  font-size: 0.95rem;
}
.nw-discovery-action {
  display: inline-block;
  background: var(--nw-char);
  color: var(--nw-surface-lowest) !important;
  padding: 8px 16px;
  border-radius: 999px;
  font-size: 0.6rem;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 0.1em;
  text-align: center;
  cursor: default;
  opacity: 0.85;
}
</style>
""",
    unsafe_allow_html=True,
)


# ---------------------------------------------------------------------------
# Main render
# ---------------------------------------------------------------------------


def main() -> None:
    last_refresh = load_last_refresh()
    refresh_value = (
        last_refresh.strftime("%b %d, %H:%M UTC")
        if last_refresh
        else "No harvest yet"
    )

    render_page_header(
        title="🥤 Beverage Trends",
        subtitle="Market intelligence for the modern beverage landscape.",
        refresh_value=refresh_value,
    )

    # Category segmented control (own row, full-width)
    category = st.radio(
        "Category",
        ["All beverages", "Functional", "THC / intoxicating"],
        horizontal=True,
        label_visibility="collapsed",
    )
    # The category toggle is a lightweight affordance; views don't slice
    # by category yet (drives off entities.category which isn't joined
    # into the trend views — future work). Kept so Lisa sees the control.
    _ = category

    # Hero card
    render_headline_hero(load_headline_mover())

    # 2x2 trend grid
    trending_flavors = load_trending(("flavor", "ingredient"))
    viral = load_viral(("brand", "ingredient", "flavor", "category"))
    trending_brands = load_trending(("brand",))
    declining = load_declining(("brand", "ingredient", "flavor", "category"))

    # Guardrail: if any trending/declining row has recent_7d > 0 but
    # prior_avg_7d == 0/NULL, the v_trending_28d / v_declining_28d views
    # are broken — fail loud instead of silently rendering "+∞% growth".
    prior_checks: list[KpiCheck] = []
    for label, df in (("trending flavors", trending_flavors),
                      ("trending brands", trending_brands),
                      ("declining", declining)):
        if df is not None and not df.empty and {"recent_7d", "prior_avg_7d"} <= set(df.columns):
            prior_checks.append(KpiCheck(
                name=f"{label} — prior_avg_7d column sum",
                current=float(df["recent_7d"].fillna(0).sum()),
                prior=float(df["prior_avg_7d"].fillna(0).sum()),
                source=f"v_trending_28d / v_declining_28d ({label})",
            ))
    validate_kpis(prior_checks)

    col1, col2 = st.columns(2, gap="large")
    with col1:
        render_card(
            title="Trending Ingredients",
            material_icon="trending_up",
            icon_color="green",
            eyebrow="Global Data",
            body_html=_trend_card_body(trending_flavors, "growth_rate", "pos"),
        )
    with col2:
        render_card(
            title="Viral This Week",
            material_icon="shutter_speed",
            icon_color="yellow",
            eyebrow="Velocity Spike",
            body_html=_trend_card_body(viral, "spike_ratio", "viral"),
        )

    col3, col4 = st.columns(2, gap="large")
    with col3:
        render_card(
            title="Trending Brands",
            material_icon="verified",
            icon_color="navy",
            eyebrow="Market Share",
            body_html=_trend_card_body(trending_brands, "growth_rate", "pos"),
        )
    with col4:
        render_card(
            title="Losing Traction",
            material_icon="south_east",
            icon_color="pink",
            eyebrow="Cooling Down",
            body_html=_trend_card_body(declining, "growth_rate", "neg"),
        )

    # Staying power
    render_staying_power_section(load_staying_power())

    # Drill-down
    combined = pd.concat(
        [trending_flavors, trending_brands], ignore_index=True
    ).drop_duplicates(subset=["entity_name"])
    render_drilldown_section(combined)

    # Discovery
    render_discovery_section(load_ngram_candidates())

    # Footer
    render_footer("Data harvested 3×/day via self-hosted SearXNG · zero-cost pipeline")


main()
