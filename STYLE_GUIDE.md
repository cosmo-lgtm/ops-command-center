# Nowadays Editorial Style — Official Dashboard System

This is the **official Streamlit dashboard style** for Nowadays. Every new
page in `ops-command-center/pages/` (and any future dashboards in
`dashboards/`) should use it. Existing dark-themed pages will be retrofit
on demand.

The reference implementation lives at
[`pages/16_Beverage_Trends.py`](pages/16_Beverage_Trends.py) — open it for a
working example of every component.

The style is delivered as a single Python module:
[`nowadays_ui.py`](nowadays_ui.py).

---

## Quick start

Every page that should look "official" needs three things:

```python
import streamlit as st
from nowadays_ui import inject_editorial_style, render_page_header, render_footer

st.set_page_config(page_title="My Dashboard", layout="wide", initial_sidebar_state="collapsed")
inject_editorial_style()                    # 1. injects CSS, fonts, palette

render_page_header(                          # 2. editorial title block
    title="🥤 My Dashboard",
    subtitle="One-line description of what this page tells you.",
    refresh_value="Apr 07, 23:45 UTC",
)

# ... your content ...

render_footer("Some small uppercase tagline")  # 3. editorial footer
```

That's it. The page is now branded.

---

## Design tokens

| Token | Value | Purpose |
|-|-|-|
| `--nw-cream` | `#E7B78A` | Brand accent, page gradient |
| `--nw-mist` | `#D7D2CB` | Brand accent, page gradient |
| `--nw-white` | `#FFFFFF` | White |
| `--nw-char` | `#2D2926` | Primary text, charcoal pill backgrounds |
| `--nw-yellow` | `#F4C864` | Viral / spike chips, hero eyebrows |
| `--nw-pink` | `#FE99A9` | Decline / negative chips |
| `--nw-green` | `#85C79D` | Growth / positive chips |
| `--nw-sky` | `#8EDDED` | Progress bar gradient start |
| `--nw-forest` | `#3F634E` | Forest text on green chips |
| `--nw-navy` | `#074A7A` | Steady chips, progress bar gradient end |
| `--nw-surface` | `#fef9f1` | Editorial canvas |
| `--nw-surface-lowest` | `#ffffff` | Card backgrounds |
| `--nw-surface-low` | `#f9f3ea` | Hover row, segmented control |
| `--nw-surface-container` | `#f3ede4` | Source cards, alt-row backgrounds |
| `--nw-surface-variant` | `#e8e2d6` | Type badge backgrounds |
| `--nw-on-surface-variant` | `#625f56` | Subtitle / secondary text |
| `--nw-outline` | `#7e7a71` | Eyebrow labels, table headers |
| `--nw-shadow` | `0 6px 24px rgba(45,41,38,0.08)` | Card shadow |
| `--nw-shadow-lg` | `0 12px 36px rgba(45,41,38,0.12)` | Hero shadow |

**Fonts:** Jost (display + body), Material Symbols Outlined (icons).
Loaded via `@import` inside the injected `<style>` block — no `<link>`
tags needed (Streamlit's HTML sanitizer strips them).

---

## Components

### Page header

```python
render_page_header(
    title="🥤 Beverage Trends",
    subtitle="Market intelligence for the modern beverage landscape.",
    refresh_value="Apr 07, 23:45 UTC",  # optional
    refresh_label="Last Refreshed",     # optional, default shown
)
```

Renders a 5/2 column split with the title + tagline on the left and the
refresh stamp on the right.

### Hero card

```python
render_hero(
    title="🔥 Energy drink is up 340% across 15 platforms this week",
    subtitle="1,060 mentions in the last 7 days · category leader",
    eyebrow="Biggest Mover",
    eyebrow_icon="bolt",                                # any Material Symbol ligature
    image_path="beverage-trends/hero-energy-splash.png",  # relative to static/
)
```

Full-bleed background image with a charcoal left-to-right gradient and
a yellow eyebrow pill. The image lives in
`dashboards/ops-command-center/static/<page-or-shared>/<file>` and is
served by Streamlit's `enableStaticServing` (already on in
`.streamlit/config.toml`).

If you don't have a hero image, omit `image_path` — the hero falls back
to a solid charcoal background.

### Card (the workhorse)

```python
render_card(
    title="Trending Flavors",
    material_icon="trending_up",   # any Material Symbol ligature
    icon_color="green",            # green / yellow / pink / navy / cream / sky
    eyebrow="Global Data",         # small uppercase label, top-right of card
    body_html=some_html_string,    # the body — any HTML you want
)
```

The body is HTML you assemble yourself — typically a sequence of
`render_row()` calls or a `<table class='nw-table'>...</table>` for
tabular data.

### Ranked rows (inside cards)

```python
from nowadays_ui import render_row, growth_chip

rows = []
for i, item in enumerate(top_items):
    rows.append(render_row(
        rank=i + 1,
        entity=item["name"],
        badge=item["category"],          # → small uppercase pill
        chip_html=growth_chip("+340%", kind="pos"),
    ))

render_card(
    title="Trending Flavors",
    material_icon="trending_up",
    body_html="".join(rows),
)
```

`growth_chip(text, kind=...)` produces a colored pill:
- `kind="pos"` → green (growth)
- `kind="neg"` → pink (decline)
- `kind="viral"` → yellow (spike)
- `kind="steady"` → navy on white (stable)

### Tables (leaderboards)

For dense structured data, build an HTML table with class `nw-table`:

```python
from nowadays_ui import progress_bar, chip, type_badge

rows = []
rows.append("<thead><tr><th>Rank</th><th>Entity</th><th class='center'>Score</th><th>Bar</th></tr></thead><tbody>")
for i, r in enumerate(df.itertuples()):
    rows.append(
        f"<tr>"
        f"<td class='nw-table-rank'>{i+1:02d}</td>"
        f"<td><div class='nw-table-entity-cell'>"
        f"<span class='nw-table-entity-name'>{r.name}</span>"
        f"{type_badge(r.kind)}"
        f"</div></td>"
        f"<td class='nw-table-score'>{r.score}<span class='nw-table-score-suffix'>/100</span></td>"
        f"<td>{progress_bar(r.score)}</td>"
        f"</tr>"
    )
rows.append("</tbody>")
table_html = f"<table class='nw-table'>{''.join(rows)}</table>"

render_card(title="Leaderboard", material_icon="leaderboard", body_html=table_html)
```

Or use `render_full_section()` for a full-width section with no card
wrapper but the same header treatment.

### Section header (no card)

```python
from nowadays_ui import render_section_header
header_html = render_section_header(
    "Deep Dive",
    legend=[("Steady", "var(--nw-navy)"), ("Growing", "var(--nw-green)")],
)
st.markdown(header_html, unsafe_allow_html=True)
```

Returns an HTML string (not rendered) so you can compose it inside a
card or another container.

### Segmented control (category toggle)

Use a regular `st.radio` with `horizontal=True, label_visibility="collapsed"`.
The injected CSS automatically converts it into a pill-style segmented
control with charcoal-on-white active state:

```python
category = st.radio(
    "Category",
    ["All", "Functional", "THC"],
    horizontal=True,
    label_visibility="collapsed",
)
```

### Footer

```python
render_footer("Data harvested 3×/day via SearXNG · zero-cost pipeline")
```

Single uppercase line, centered, divider on top.

---

## Component naming

Every CSS class in the system is prefixed `nw-*` (Nowadays). The `bt-*`
prefix used in earlier drafts was Beverage-Trends-specific and has been
fully retired. If you need a page-specific component that doesn't fit
any helper, declare its CSS in a small `<style>` block at the top of
your page and use a `nw-page-*` prefix to avoid collisions.

| Class | Used by |
|-|-|
| `nw-page-title` / `nw-page-subtitle` | `render_page_header()` |
| `nw-refresh-eyebrow` / `nw-refresh-time` | `render_page_header()` |
| `nw-hero` / `nw-hero-bg` / `nw-hero-overlay` / `nw-hero-content` | `render_hero()` |
| `nw-hero-eyebrow` / `nw-hero-title` / `nw-hero-sub` | `render_hero()` |
| `nw-card` / `nw-card-header` / `nw-card-title` / `nw-card-eyebrow` | `render_card()` |
| `nw-icon-{green,yellow,pink,navy,cream,sky}` | `render_card(icon_color=...)` |
| `nw-row` / `nw-rank` / `nw-entity` / `nw-empty-row` | `render_row()` / `render_empty_row()` |
| `nw-type-badge` | `type_badge()` |
| `nw-chip-{pos,neg,viral,steady}` | `growth_chip()` |
| `nw-chip` / `nw-chip-on` | `chip()` |
| `nw-section-header` / `nw-section-title` / `nw-section-legend` / `nw-legend-item` | `render_section_header()` |
| `nw-table` / `nw-table-rank` / `nw-table-entity-cell` / `nw-table-entity-name` / `nw-table-score` | manual table |
| `nw-bar-wrap` / `nw-bar-fill` | `progress_bar()` |
| `nw-source-grid` / `nw-source-card` / `nw-source-pill` / `nw-source-title` / `nw-source-snippet` / `nw-source-link` | manual source-card grid |
| `nw-footer` | `render_footer()` |

---

## Spacing rules

- Page max-width: **1440px**
- Page horizontal padding: **3rem** (48px)
- Card padding: **40px 44px**
- Card border-radius: **24px**
- Inter-section vertical gap: **1.75rem** (auto-applied via stVerticalBlock)
- Card header → body margin: **32px**
- Row vertical padding: **16px**
- Section header → content margin: **32px**

---

## Adopting the style on an existing dashboard

The style is opt-in per page — just import `nowadays_ui` and call
`inject_editorial_style()`. The CSS is scoped under `[data-testid="stApp"]`
selectors so it only affects the page that imports it; other pages with
their own dark theme are untouched.

Retrofit checklist for an existing page:

1. `from nowadays_ui import inject_editorial_style, render_page_header, render_card, render_footer`
2. Call `inject_editorial_style()` after `st.set_page_config(...)`
3. Replace any custom `st.markdown(<h1>...)` with `render_page_header(...)`
4. Wrap each major content block in `render_card(...)` or build the body
   yourself with `nw-*` classes
5. Add `render_footer(...)` at the bottom
6. Delete any `st.markdown("<style>...</style>")` blocks the page used to
   ship — the editorial style supersedes them
7. Test locally, verify nothing visually regresses, then ship

---

## When to add a new component to the module

If you find yourself building the same HTML shape on two different
pages, **promote it into `nowadays_ui.py`**. The Beverage Trends page
keeps a small page-local `<style>` block for its discovery feed grid
because no other dashboard uses that layout yet — when a second
dashboard needs it, that block moves into `nowadays_ui.py` and gets
its own `nw-discovery-*` classes promoted.

Rules of thumb:
- **One page uses it** → keep it page-local
- **Two pages use it** → promote to `nowadays_ui.py`, remove the
  page-local copies in the same commit
- **Naming** — always `nw-*` prefix for module classes,
  `nw-page-{name}-*` for page-locals to avoid future collision

---

## Static images

Streamlit serves files under `dashboards/ops-command-center/static/`
at the URL path `/app/static/...` because `enableStaticServing = true`
is set in `.streamlit/config.toml`. Reference them from CSS as:

```css
background-image: url('app/static/<page-name>/<file>.png');
```

Or pass the relative path to `render_hero(image_path="...")`:

```python
render_hero(
    ...,
    image_path="beverage-trends/hero-energy-splash.png",  # no /app/static/ prefix
)
```

The helper handles the URL construction.

Keep page-specific images under
`static/<page-slug>/` and shared/brand assets under `static/shared/`.

---

## Attribution

The visual language was prototyped in [Google Stitch](https://stitch.withgoogle.com/)
and ported to Streamlit. The original Stitch HTML lives in
`scratch/stitch-export/` for reference. The hero energy-splash image
was generated by Stitch and downloaded into the static folder.
