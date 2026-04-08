"""
Nowadays Editorial UI — official Streamlit dashboard style.

Usage in a page file:

    import streamlit as st
    from nowadays_ui import (
        inject_editorial_style,
        render_page_header,
        render_hero,
        render_section_header,
        render_card,
        render_footer,
        type_badge,
        growth_chip,
        chip,
    )

    st.set_page_config(page_title="My Dashboard", layout="wide", ...)
    inject_editorial_style()

    render_page_header(
        title="🥤 My Dashboard",
        subtitle="One-line description of what this page tells you.",
        refresh_value="Apr 07, 23:45 UTC",
    )

    render_hero(
        title="Energy drink is up 340% across 15 platforms this week",
        subtitle="1,060 mentions in the last 7 days · category leader",
        eyebrow="Biggest Mover",
        eyebrow_icon="bolt",
        image_path="hero-energy-splash.png",
    )

    render_card(
        title="Trending Flavors",
        material_icon="trending_up",
        icon_color="green",
        eyebrow="Global Data",
        body_html="<div class='nw-row'>...</div>",
    )

    render_footer("Data harvested 3×/day via SearXNG · zero-cost pipeline")

Conventions:
- Container: page is centered at max-width 1440px on a warm cream gradient.
- Type: Jost everywhere (display + body), Material Symbols Outlined for icons.
- Cards: white surface, soft shadow, 24px radius, 32px internal padding.
- Spacing: ~24-32px gap between sections.
- Color accents follow the Nowadays brand palette
  (cream / mist / yellow / pink / green / sky / forest / navy / charcoal).

The CSS classes are all prefixed `nw-*` so this module can coexist with any
other Streamlit page-level CSS without collisions. The full class list is
documented in dashboards/ops-command-center/STYLE_GUIDE.md.

Static assets (hero images, etc.) live in
`dashboards/ops-command-center/static/<page-or-shared>/` and are referenced
from CSS as `url('app/static/<page>/<file>')`. Streamlit Cloud serves them
because `enableStaticServing = true` is set in `.streamlit/config.toml`.
"""

from __future__ import annotations

from typing import Iterable

import streamlit as st


# ---------------------------------------------------------------------------
# CSS
# ---------------------------------------------------------------------------

EDITORIAL_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Jost:wght@300;400;500;600;700&family=Material+Symbols+Outlined:opsz,wght,FILL,GRAD@24,400,0,0&display=swap');

:root {
  /* Nowadays brand accents (per Lisa's spec) */
  --nw-cream:  #E7B78A;
  --nw-mist:   #D7D2CB;
  --nw-white:  #FFFFFF;
  --nw-char:   #2D2926;
  --nw-yellow: #F4C864;
  --nw-pink:   #FE99A9;
  --nw-green:  #85C79D;
  --nw-sky:    #8EDDED;
  --nw-forest: #3F634E;
  --nw-navy:   #074A7A;

  /* Editorial surface canvas (warmer than pure white — harmonizes with cream) */
  --nw-surface: #fef9f1;
  --nw-surface-lowest: #ffffff;
  --nw-surface-low: #f9f3ea;
  --nw-surface-container: #f3ede4;
  --nw-surface-high: #eee7dd;
  --nw-surface-variant: #e8e2d6;
  --nw-on-surface-variant: #625f56;
  --nw-outline: #7e7a71;
  --nw-outline-variant: #b7b1a7;

  /* Editorial shadow */
  --nw-shadow: 0 6px 24px rgba(45, 41, 38, 0.08);
  --nw-shadow-lg: 0 12px 36px rgba(45, 41, 38, 0.12);
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

[data-testid="stApp"] {
  background: linear-gradient(135deg, var(--nw-mist) 0%, var(--nw-surface) 50%, var(--nw-cream) 100%) !important;
  background-attachment: fixed !important;
}

[data-testid="stMainBlockContainer"] {
  max-width: 1440px !important;
  padding-top: 2.5rem !important;
  padding-left: 3rem !important;
  padding-right: 3rem !important;
  padding-bottom: 5rem !important;
}

/* Streamlit's default vertical block has zero gap between sections;
   add explicit breathing room so cards feel editorial, not cramped. */
[data-testid="stMain"] [data-testid="stVerticalBlock"] {
  gap: 1.75rem !important;
}

/* Default text colors — wrapped in :where() so they have ZERO specificity
   and any class-targeted color rule wins. */
:where([data-testid="stMain"] h1),
:where([data-testid="stMain"] h2),
:where([data-testid="stMain"] h3),
:where([data-testid="stMain"] h4) {
  color: var(--nw-char);
  letter-spacing: -0.025em;
  font-weight: 700;
}
:where([data-testid="stMain"] [data-testid="stMarkdownContainer"] p),
:where([data-testid="stMain"] [data-testid="stMarkdownContainer"] span),
:where([data-testid="stMain"] [data-testid="stMarkdownContainer"] div) {
  color: var(--nw-char);
}

/* Hide streamlit chrome that fights the editorial vibe */
[data-testid="stHeader"] { background: transparent !important; }
[data-testid="stDecoration"] { display: none !important; }
.stDeployButton { display: none !important; }

/* ----------------------------------------------------------------------
   PAGE HEADER + DASHBOARD CONTROL BAR
   ---------------------------------------------------------------------- */

.nw-page-title {
  font-size: 3.5rem !important;
  font-weight: 700 !important;
  letter-spacing: -0.035em !important;
  line-height: 1 !important;
  color: var(--nw-char) !important;
  margin: 0 !important;
}
.nw-page-subtitle {
  color: var(--nw-on-surface-variant) !important;
  font-weight: 500;
  font-size: 1rem;
  margin-top: 0.4rem;
}
.nw-refresh-eyebrow {
  font-size: 0.7rem;
  text-transform: uppercase;
  letter-spacing: 0.18em;
  color: var(--nw-outline);
  font-weight: 600;
}
.nw-refresh-time {
  font-weight: 600;
  color: var(--nw-char);
  font-size: 0.95rem;
  margin-top: 2px;
}

/* Segmented control — pill-shaped radio for category filters */
[data-testid="stRadio"] [role="radiogroup"] {
  background: var(--nw-surface-low) !important;
  border-radius: 999px !important;
  padding: 6px !important;
  box-shadow: var(--nw-shadow);
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
[data-testid="stRadio"] [role="radiogroup"] label,
[data-testid="stRadio"] [role="radiogroup"] label *:not(input) {
  color: var(--nw-char) !important;
  font-weight: 600 !important;
  font-size: 0.85rem !important;
  font-family: 'Jost', 'Helvetica', sans-serif !important;
}
[data-testid="stRadio"] [role="radiogroup"] label:has(input:checked) {
  background: var(--nw-char) !important;
}
[data-testid="stRadio"] [role="radiogroup"] label:has(input:checked),
[data-testid="stRadio"] [role="radiogroup"] label:has(input:checked) *:not(input) {
  color: #ffffff !important;
}
[data-testid="stRadio"] [role="radiogroup"] label > div:first-child { display: none !important; }

/* Streamlit selectbox — nudge to match the editorial vibe */
[data-testid="stSelectbox"] > div > div {
  background: var(--nw-surface-lowest) !important;
  border-radius: 999px !important;
  border: 1px solid var(--nw-surface-variant) !important;
  box-shadow: var(--nw-shadow);
}

/* ----------------------------------------------------------------------
   HERO CARD — full-bleed image with charcoal gradient overlay
   ---------------------------------------------------------------------- */

.nw-hero {
  position: relative;
  border-radius: 28px;
  overflow: hidden;
  height: 440px;
  box-shadow: var(--nw-shadow-lg);
  margin: 28px 0 40px 0;
  display: flex;
  align-items: flex-end;
  background: var(--nw-char);
}
.nw-hero-bg {
  position: absolute;
  inset: 0;
  background-size: 60% auto;
  background-position: right center;
  background-repeat: no-repeat;
  filter: saturate(1.2) contrast(1.05);
  opacity: 0.95;
}
.nw-hero-overlay {
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
.nw-hero-content {
  position: relative;
  z-index: 2;
  padding: 56px;
  max-width: 880px;
}
.nw-hero-content,
.nw-hero-content * {
  color: #ffffff !important;
}
.nw-hero-eyebrow {
  display: inline-flex !important;
  align-items: center;
  gap: 6px;
  background: var(--nw-yellow);
  color: var(--nw-char) !important;
  padding: 7px 16px;
  border-radius: 999px;
  font-size: 0.7rem;
  font-weight: 700 !important;
  text-transform: uppercase;
  letter-spacing: 0.14em;
  margin-bottom: 22px;
}
.nw-hero-eyebrow .material-symbols-outlined { color: var(--nw-char) !important; }
.nw-hero-title {
  font-size: 3.6rem !important;
  font-weight: 700 !important;
  letter-spacing: -0.035em !important;
  line-height: 1.04 !important;
  margin: 0 0 16px 0 !important;
  color: #ffffff !important;
  text-shadow: 0 2px 32px rgba(0, 0, 0, 0.55);
}
.nw-hero-sub {
  font-size: 1.1rem !important;
  font-weight: 500 !important;
  color: rgba(255, 255, 255, 0.92) !important;
  text-shadow: 0 1px 16px rgba(0, 0, 0, 0.55);
  margin: 0 !important;
}

/* ----------------------------------------------------------------------
   CARDS — generic editorial card wrapper
   ---------------------------------------------------------------------- */

.nw-card {
  background: var(--nw-surface-lowest);
  border-radius: 24px;
  padding: 40px 44px;
  box-shadow: var(--nw-shadow);
  margin-bottom: 0;
  border: 1px solid rgba(45, 41, 38, 0.04);
}
.nw-card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 32px;
}
.nw-card-title {
  font-size: 1.5rem !important;
  font-weight: 700 !important;
  letter-spacing: -0.02em !important;
  color: var(--nw-char) !important;
  display: flex;
  align-items: center;
  gap: 14px;
  margin: 0 !important;
}
.nw-card-title .material-symbols-outlined {
  font-size: 30px !important;
}
.nw-card-eyebrow {
  font-size: 0.65rem;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.16em;
  color: var(--nw-outline);
}

/* Icon color helpers */
.nw-icon-green { color: var(--nw-forest); }
.nw-icon-yellow { color: var(--nw-char); }
.nw-icon-pink { color: #b04d5e; }
.nw-icon-navy { color: var(--nw-navy); }
.nw-icon-cream { color: var(--nw-cream); }
.nw-icon-sky { color: var(--nw-navy); }

/* ----------------------------------------------------------------------
   ROWS — ranked list rows (e.g. trend lists)
   ---------------------------------------------------------------------- */

.nw-row {
  display: grid;
  grid-template-columns: 36px 1fr auto auto;
  gap: 18px;
  align-items: center;
  padding: 16px 12px;
  margin: 0 -12px;
  border-radius: 12px;
  transition: background 0.15s ease;
}
.nw-row + .nw-row { border-top: 1px solid rgba(45, 41, 38, 0.05); }
.nw-row:hover { background: var(--nw-surface-low); }
.nw-rank {
  font-weight: 700;
  font-size: 0.85rem;
  color: var(--nw-outline);
  letter-spacing: 0.02em;
}
.nw-entity {
  font-weight: 600;
  font-size: 1.05rem;
  color: var(--nw-char);
}

/* Empty-state row */
.nw-empty-row {
  padding: 18px 8px;
  color: var(--nw-on-surface-variant);
  font-style: italic;
  font-size: 0.95rem;
}

/* ----------------------------------------------------------------------
   BADGES, CHIPS, PILLS
   ---------------------------------------------------------------------- */

.nw-type-badge {
  font-size: 0.62rem;
  padding: 3px 9px;
  border-radius: 999px;
  background: var(--nw-surface-variant);
  color: var(--nw-on-surface-variant);
  text-transform: uppercase;
  letter-spacing: 0.06em;
  font-weight: 700;
}

.nw-chip-pos {
  font-weight: 700;
  font-size: 0.78rem;
  background: var(--nw-green);
  color: var(--nw-forest);
  padding: 5px 12px;
  border-radius: 999px;
  letter-spacing: 0.02em;
}
.nw-chip-neg {
  font-weight: 700;
  font-size: 0.78rem;
  background: var(--nw-pink);
  color: var(--nw-char);
  padding: 5px 12px;
  border-radius: 999px;
  letter-spacing: 0.02em;
}
.nw-chip-viral {
  font-weight: 700;
  font-size: 0.78rem;
  background: var(--nw-yellow);
  color: var(--nw-char);
  padding: 5px 12px;
  border-radius: 999px;
  letter-spacing: 0.02em;
}
.nw-chip-steady {
  font-weight: 700;
  font-size: 0.78rem;
  background: var(--nw-navy);
  color: var(--nw-white);
  padding: 5px 12px;
  border-radius: 999px;
  letter-spacing: 0.02em;
}

.nw-chip {
  display: inline-block;
  font-size: 0.6rem;
  padding: 4px 10px;
  border-radius: 999px;
  background: var(--nw-surface-variant);
  color: var(--nw-on-surface-variant);
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 0.06em;
  margin-right: 6px;
}
.nw-chip-on {
  background: var(--nw-green);
  color: var(--nw-forest);
}

/* ----------------------------------------------------------------------
   SECTION HEADER — used outside cards (e.g. "Deep Dive", "Discovery")
   ---------------------------------------------------------------------- */

.nw-section-header {
  display: flex;
  justify-content: space-between;
  align-items: flex-end;
  margin-bottom: 32px;
}
.nw-section-title {
  font-size: 2rem !important;
  font-weight: 700 !important;
  letter-spacing: -0.025em !important;
  color: var(--nw-char) !important;
  margin: 0 !important;
}
.nw-section-legend {
  display: flex;
  gap: 18px;
}
.nw-legend-item {
  display: flex;
  align-items: center;
  gap: 6px;
  font-size: 0.7rem;
  text-transform: uppercase;
  letter-spacing: 0.1em;
  font-weight: 600;
  color: var(--nw-on-surface-variant);
}
.nw-legend-dot {
  width: 10px;
  height: 10px;
  border-radius: 999px;
}

/* ----------------------------------------------------------------------
   EDITORIAL TABLE — for leaderboards and structured data
   ---------------------------------------------------------------------- */

.nw-table {
  width: 100%;
  border-collapse: collapse;
}
.nw-table thead th {
  text-align: left;
  font-size: 0.65rem;
  text-transform: uppercase;
  letter-spacing: 0.14em;
  color: var(--nw-outline);
  font-weight: 700;
  padding: 0 12px 14px 12px;
  border-bottom: 1px solid var(--nw-surface-variant);
}
.nw-table thead th.center { text-align: center; }
.nw-table thead th.right { text-align: right; }
.nw-table tbody tr {
  transition: background 0.15s ease;
}
.nw-table tbody tr:hover { background: var(--nw-surface-low); }
.nw-table tbody td {
  padding: 22px 12px;
  border-bottom: 1px solid rgba(45, 41, 38, 0.04);
  vertical-align: middle;
}
.nw-table-rank {
  font-weight: 700;
  font-size: 1rem;
  color: var(--nw-char);
  width: 50px;
}
.nw-table-entity-cell {
  display: flex;
  align-items: center;
  gap: 14px;
}
.nw-table-entity-name {
  font-weight: 700;
  font-size: 1.1rem;
  color: var(--nw-char);
}
.nw-table-score {
  text-align: center;
  font-weight: 700;
  font-size: 1.5rem;
  color: var(--nw-char);
  width: 110px;
}
.nw-table-score-suffix {
  font-size: 0.7rem;
  color: var(--nw-outline);
  font-weight: 500;
}

/* Progress bar (used inside table cells) */
.nw-bar-wrap {
  width: 100%;
  background: var(--nw-surface-high);
  border-radius: 999px;
  height: 10px;
  overflow: hidden;
}
.nw-bar-fill {
  height: 100%;
  background: linear-gradient(90deg, var(--nw-sky), var(--nw-navy));
  border-radius: 999px;
  transition: width 0.4s ease;
}

/* ----------------------------------------------------------------------
   SOURCE CARD GRID — drill-down content cards
   ---------------------------------------------------------------------- */

.nw-source-grid {
  display: grid;
  grid-template-columns: repeat(5, minmax(0, 1fr));
  gap: 20px;
}
.nw-source-card {
  background: var(--nw-surface-container);
  border-radius: 20px;
  padding: 24px;
  display: flex;
  flex-direction: column;
  justify-content: space-between;
  min-height: 280px;
  box-shadow: var(--nw-shadow);
  transition: transform 0.18s ease, box-shadow 0.18s ease;
}
.nw-source-card:hover {
  transform: translateY(-3px);
  box-shadow: var(--nw-shadow-lg);
}
.nw-source-pill {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  background: var(--nw-surface-lowest);
  padding: 5px 12px;
  border-radius: 999px;
  font-size: 0.6rem;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 0.08em;
  color: var(--nw-on-surface-variant);
  box-shadow: 0 1px 4px rgba(45, 41, 38, 0.06);
  width: fit-content;
}
.nw-source-title {
  font-weight: 700;
  font-size: 1rem;
  color: var(--nw-char);
  line-height: 1.3;
  margin-top: 14px;
}
.nw-source-snippet {
  font-size: 0.82rem;
  color: var(--nw-on-surface-variant);
  line-height: 1.5;
  margin-top: 10px;
  display: -webkit-box;
  -webkit-line-clamp: 4;
  -webkit-box-orient: vertical;
  overflow: hidden;
}
.nw-source-link {
  color: var(--nw-navy) !important;
  font-size: 0.65rem;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 0.12em;
  margin-top: 18px;
  display: inline-flex;
  align-items: center;
  gap: 4px;
  text-decoration: none;
}
.nw-source-link:hover { text-decoration: underline; }

/* ----------------------------------------------------------------------
   FOOTER
   ---------------------------------------------------------------------- */

.nw-footer {
  text-align: center;
  margin-top: 48px;
  padding: 24px;
  color: var(--nw-outline);
  font-size: 0.7rem;
  text-transform: uppercase;
  letter-spacing: 0.14em;
  font-weight: 600;
  border-top: 1px solid var(--nw-surface-variant);
}
</style>
"""


# ---------------------------------------------------------------------------
# Style injection
# ---------------------------------------------------------------------------


def inject_editorial_style() -> None:
    """Inject the editorial CSS, fonts, and material symbols on the current page.

    Call this once at the top of every page that should use the Nowadays
    editorial style, immediately after `st.set_page_config(...)`.
    """
    st.markdown(EDITORIAL_CSS, unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Layout helpers
# ---------------------------------------------------------------------------


def render_page_header(
    title: str,
    subtitle: str | None = None,
    refresh_label: str = "Last Refreshed",
    refresh_value: str | None = None,
) -> None:
    """Render the editorial page header (title + subtitle on left, optional
    refresh stamp on right). Uses two Streamlit columns internally.
    """
    col_head, col_refresh = st.columns([5, 2])
    with col_head:
        sub_html = (
            f"<p class='nw-page-subtitle'>{subtitle}</p>" if subtitle else ""
        )
        st.markdown(
            f"<h1 class='nw-page-title'>{title}</h1>{sub_html}",
            unsafe_allow_html=True,
        )
    with col_refresh:
        if refresh_value:
            st.markdown(
                "<div style='text-align:right; padding-top:14px;'>"
                f"<div class='nw-refresh-eyebrow'>{refresh_label}</div>"
                f"<div class='nw-refresh-time'>{refresh_value}</div>"
                "</div>",
                unsafe_allow_html=True,
            )


def render_hero(
    title: str,
    subtitle: str | None = None,
    eyebrow: str | None = None,
    eyebrow_icon: str | None = None,
    image_path: str | None = None,
    image_url: str | None = None,
) -> None:
    """Render the full-bleed hero card.

    Pass either `image_path` (relative to the static/ folder, e.g.
    "beverage-trends/hero-energy-splash.png") OR `image_url` (full URL).
    If both are None, the hero renders with a solid charcoal background.
    """
    bg_url = ""
    if image_path:
        bg_url = f"url('app/static/{image_path}')"
    elif image_url:
        bg_url = f"url('{image_url}')"

    bg_style = f"background-image: {bg_url};" if bg_url else ""

    eyebrow_html = ""
    if eyebrow:
        icon = ""
        if eyebrow_icon:
            icon = (
                f"<span class='material-symbols-outlined' style='font-size:14px;'>"
                f"{eyebrow_icon}</span>"
            )
        eyebrow_html = f"<div class='nw-hero-eyebrow'>{icon}{eyebrow}</div>"

    sub_html = f"<p class='nw-hero-sub'>{subtitle}</p>" if subtitle else ""

    hero_html = (
        "<div class='nw-hero'>"
        f"<div class='nw-hero-bg' style=\"{bg_style}\"></div>"
        "<div class='nw-hero-overlay'></div>"
        "<div class='nw-hero-content'>"
        f"{eyebrow_html}"
        f"<h2 class='nw-hero-title'>{title}</h2>"
        f"{sub_html}"
        "</div>"
        "</div>"
    )
    st.markdown(hero_html, unsafe_allow_html=True)


def render_section_header(
    title: str,
    legend: list[tuple[str, str]] | None = None,
) -> str:
    """Return (don't render) a section header HTML chunk.

    `legend` is an optional list of `(label, color_hex)` tuples that render
    as colored dots + uppercase labels on the right side. Returns the HTML
    so the caller can compose it inside a card or section.

    For a top-level section header (not inside a card), wrap the result in
    a `<div class='nw-card'>...</div>` or render directly via st.markdown.
    """
    legend_html = ""
    if legend:
        items = "".join(
            f"<div class='nw-legend-item'>"
            f"<span class='nw-legend-dot' style='background:{color};'></span>"
            f"{label}</div>"
            for label, color in legend
        )
        legend_html = f"<div class='nw-section-legend'>{items}</div>"

    return (
        "<div class='nw-section-header'>"
        f"<h3 class='nw-section-title'>{title}</h3>"
        f"{legend_html}"
        "</div>"
    )


def render_card(
    title: str,
    body_html: str,
    material_icon: str | None = None,
    icon_color: str = "navy",
    eyebrow: str | None = None,
) -> None:
    """Render a generic editorial card.

    Args:
        title: Card title (will be rendered as h3 in Jost).
        body_html: The card body as a single HTML string.
        material_icon: Optional Material Symbols Outlined ligature name
            (e.g. "trending_up", "verified", "shutter_speed").
        icon_color: One of "green", "yellow", "pink", "navy", "cream",
            "sky" — controls the icon color via the nw-icon-* helper.
        eyebrow: Optional small uppercase label rendered on the right
            of the card header (e.g. "Global Data", "Velocity Spike").
    """
    icon_html = ""
    if material_icon:
        icon_html = (
            f"<span class='material-symbols-outlined nw-icon-{icon_color}'>"
            f"{material_icon}</span>"
        )
    eyebrow_html = (
        f"<div class='nw-card-eyebrow'>{eyebrow}</div>" if eyebrow else ""
    )
    card_html = (
        "<div class='nw-card'>"
        "<div class='nw-card-header'>"
        f"<div class='nw-card-title'>{icon_html}{title}</div>"
        f"{eyebrow_html}"
        "</div>"
        f"{body_html}"
        "</div>"
    )
    st.markdown(card_html, unsafe_allow_html=True)


def render_full_section(
    title: str,
    body_html: str,
    legend: list[tuple[str, str]] | None = None,
) -> None:
    """Render a full-width editorial section (no card wrapper but with the
    section header pattern). Used for things like "Deep Dive", "Discovery"
    that span the full content width.
    """
    header_html = render_section_header(title, legend)
    st.markdown(
        f"<div class='nw-card'>{header_html}{body_html}</div>",
        unsafe_allow_html=True,
    )


def render_footer(text: str) -> None:
    """Render the editorial footer (single uppercase line)."""
    st.markdown(
        f"<div class='nw-footer'>{text}</div>",
        unsafe_allow_html=True,
    )


# ---------------------------------------------------------------------------
# Component formatters (return HTML strings — caller composes them)
# ---------------------------------------------------------------------------


def type_badge(label: str) -> str:
    """Small uppercase pill — typically used to label entity types."""
    return f"<span class='nw-type-badge'>{label}</span>"


def growth_chip(text: str, kind: str = "pos") -> str:
    """Colored chip for growth/decline/viral metrics.

    `kind` is one of:
        "pos"    — green background, forest text (positive growth)
        "neg"    — pink background, charcoal text (decline)
        "viral"  — yellow background, charcoal text (sudden spike)
        "steady" — navy background, white text (stable)
    """
    cls = {
        "pos": "nw-chip-pos",
        "neg": "nw-chip-neg",
        "viral": "nw-chip-viral",
        "steady": "nw-chip-steady",
    }.get(kind, "nw-chip-pos")
    return f"<span class='{cls}'>{text}</span>"


def chip(text: str, active: bool = False) -> str:
    """Small generic chip (used for binary signals like "cross-platform").

    Set `active=True` to render as the green-filled "on" state.
    """
    on_class = " nw-chip-on" if active else ""
    return f"<span class='nw-chip{on_class}'>{text}</span>"


def progress_bar(percent: float) -> str:
    """Return HTML for a horizontal progress bar (0-100)."""
    pct = max(0, min(100, int(percent)))
    return (
        "<div class='nw-bar-wrap'>"
        f"<div class='nw-bar-fill' style='width:{pct}%;'></div>"
        "</div>"
    )


def render_row(
    rank: int,
    entity: str,
    badge: str | None = None,
    chip_html: str | None = None,
) -> str:
    """Return HTML for a single ranked list row.

    Composes a 4-column grid: rank · entity · type badge · metric chip.
    Returns the row HTML; caller is responsible for wrapping in a card body.
    """
    badge_html = type_badge(badge) if badge else ""
    chip_block = chip_html or ""
    return (
        "<div class='nw-row'>"
        f"<div class='nw-rank'>{rank:02d}</div>"
        f"<div class='nw-entity'>{entity}</div>"
        f"<div>{badge_html}</div>"
        f"<div>{chip_block}</div>"
        "</div>"
    )


def render_empty_row(message: str = "No data yet — check back after the next refresh.") -> str:
    """Return HTML for an empty-state placeholder row."""
    return f"<div class='nw-empty-row'>{message}</div>"
