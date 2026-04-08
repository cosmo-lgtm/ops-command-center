"""
Ops Command Center — splash / launcher.

Multi-page Streamlit dashboard consolidating all Nowadays operations
analytics. This file is the home page (splash) only — each dashboard
lives in pages/N_Name.py and uses the editorial style from
`nowadays_ui.py` per `STYLE_GUIDE.md`.
"""

import streamlit as st

from nowadays_ui import inject_editorial_style, render_footer, render_page_header

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Ops Command Center",
    page_icon="🎛️",
    layout="wide",
    initial_sidebar_state="auto",
)

inject_editorial_style()


# ---------------------------------------------------------------------------
# Splash-local CSS for the dashboard launcher tiles
# ---------------------------------------------------------------------------

st.markdown(
    """
<style>
.nw-launcher-grid {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 28px;
  margin-top: 16px;
}
.nw-launcher-card {
  background: var(--nw-surface-lowest);
  border-radius: 24px;
  padding: 32px 32px 28px 32px;
  box-shadow: var(--nw-shadow);
  border: 1px solid rgba(45, 41, 38, 0.04);
  transition: transform 0.2s ease, box-shadow 0.2s ease;
  display: flex;
  flex-direction: column;
  text-decoration: none !important;
  color: inherit !important;
  height: 100%;
  min-height: 240px;
}
.nw-launcher-card:hover {
  transform: translateY(-3px);
  box-shadow: var(--nw-shadow-lg);
}
.nw-launcher-head {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  margin-bottom: 22px;
}
.nw-launcher-icon {
  width: 52px;
  height: 52px;
  border-radius: 16px;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  font-size: 28px;
  background: var(--nw-surface-low);
}
.nw-launcher-icon.cream { background: rgba(231, 183, 138, 0.25); color: #8b5a2b; }
.nw-launcher-icon.green { background: rgba(133, 199, 157, 0.25); color: var(--nw-forest); }
.nw-launcher-icon.yellow { background: rgba(244, 200, 100, 0.25); color: #8a6b00; }
.nw-launcher-icon.pink { background: rgba(254, 153, 169, 0.25); color: #b04d5e; }
.nw-launcher-icon.sky { background: rgba(142, 221, 237, 0.25); color: var(--nw-navy); }
.nw-launcher-icon.navy { background: rgba(7, 74, 122, 0.15); color: var(--nw-navy); }
.nw-launcher-icon.mist { background: rgba(215, 210, 203, 0.45); color: var(--nw-char); }

.nw-launcher-category {
  font-size: 0.6rem;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 0.14em;
  color: var(--nw-on-surface-variant);
  background: var(--nw-surface-variant);
  padding: 5px 10px;
  border-radius: 999px;
  font-family: 'Jost', 'Helvetica', sans-serif !important;
}

.nw-launcher-title {
  font-size: 1.4rem !important;
  font-weight: 700 !important;
  letter-spacing: -0.02em !important;
  color: var(--nw-char) !important;
  margin: 0 0 8px 0 !important;
  font-family: 'Jost', 'Helvetica', sans-serif !important;
  line-height: 1.2;
}
.nw-launcher-desc {
  font-size: 0.9rem;
  color: var(--nw-on-surface-variant);
  line-height: 1.5;
  margin: 0;
  flex-grow: 1;
}
.nw-launcher-cta {
  margin-top: 22px;
  display: inline-flex;
  align-items: center;
  gap: 6px;
  color: var(--nw-navy) !important;
  font-size: 0.65rem;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 0.14em;
  font-family: 'Jost', 'Helvetica', sans-serif !important;
}

/* Featured strip — small system status banner above the grid */
.nw-status-strip {
  display: flex;
  align-items: center;
  justify-content: space-between;
  background: var(--nw-surface-lowest);
  border-radius: 999px;
  padding: 12px 24px;
  margin: 0 0 32px 0;
  box-shadow: var(--nw-shadow);
}
.nw-status-strip .left {
  display: inline-flex;
  align-items: center;
  gap: 10px;
  font-family: 'Jost', 'Helvetica', sans-serif !important;
  font-size: 0.75rem;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.12em;
  color: var(--nw-forest);
}
.nw-status-strip .live-dot {
  width: 8px;
  height: 8px;
  background: var(--nw-green);
  border-radius: 50%;
  box-shadow: 0 0 0 4px rgba(133, 199, 157, 0.25);
  animation: nwPulse 2s infinite;
}
@keyframes nwPulse {
  0%, 100% { opacity: 1; transform: scale(1); }
  50% { opacity: 0.6; transform: scale(1.15); }
}
.nw-status-strip .right {
  font-family: 'Jost', 'Helvetica', sans-serif !important;
  font-size: 0.7rem;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.14em;
  color: var(--nw-outline);
}

@media (max-width: 1100px) {
  .nw-launcher-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
}
</style>
""",
    unsafe_allow_html=True,
)


# ---------------------------------------------------------------------------
# Dashboard registry
# ---------------------------------------------------------------------------

# (slug, icon, icon_color, title, description, category)
DASHBOARDS = [
    ("Distributor_Inventory", "inventory_2", "green",
     "Distributor Inventory",
     "Salesforce orders vs VIP depletion — weeks of inventory, overstock and understock by distributor and SKU.",
     "Sales"),

    ("ShipStation_Fulfillment", "local_shipping", "sky",
     "ShipStation Fulfillment",
     "B2B order fulfillment analytics — shipping costs, carrier performance, days-to-ship, and delivery metrics.",
     "Operations"),

    ("Zendesk_Support", "support_agent", "pink",
     "Zendesk Support",
     "B2C customer support analytics — CSAT scores, resolution times, agent performance, and ticket backlog.",
     "Support"),

    ("Visit_Performance", "explore", "yellow",
     "Visit Performance",
     "Sales rep check-in attribution — visit-to-depletion conversion, POD growth tracking, route effectiveness.",
     "Sales"),

    ("Shopify_Forecast", "auto_graph", "navy",
     "Shopify Forecast",
     "DTC revenue forecasting from Shopify order history — demand projections, product mix, seasonality.",
     "DTC"),

    ("Sales_Dashboard", "trending_up", "green",
     "Sales Dashboard",
     "Field sales performance — distributor velocity, rep activity, territory coverage, year-over-year comparisons.",
     "Sales"),

    ("DTC_Metrics", "shopping_cart", "cream",
     "DTC Metrics",
     "Direct-to-consumer commerce overview — orders, AOV, conversion, channel mix across Shopify and WooCommerce.",
     "DTC"),

    ("Store_Locator", "store", "mist",
     "Store Locator (POC)",
     "Consumer-facing store finder — retailers with products ordered in the last 90 days, mapped via Google Places.",
     "POC"),

    ("Payments", "credit_card", "sky",
     "Payments",
     "Authorize.net payment analytics — transaction volume, card types, geographic distribution, US vs international.",
     "DTC"),

    ("Marketing_Scorecard", "campaign", "yellow",
     "Marketing Scorecard",
     "Cross-channel marketing performance — paid social, email, organic, attribution, and ROAS by campaign.",
     "Marketing"),

    ("KAM_Performance", "supervisor_account", "navy",
     "KAM Performance",
     "Key Account Manager performance — chain retail accounts, depletion velocity, gap analysis, and quarterly reviews.",
     "Sales"),

    ("D2C_Customer_LTV", "loyalty", "pink",
     "D2C Customer LTV",
     "Direct-to-consumer customer lifetime value — cohort retention, mature 6/12-month LTV, Shopify vs WooCommerce.",
     "DTC"),

    ("Beverage_Trends", "local_drink", "cream",
     "Beverage Trends",
     "Social + web trend signals for functional and THC beverages — trending flavors, viral brands, staying-power scoring.",
     "Marketing"),
]


# ---------------------------------------------------------------------------
# Render
# ---------------------------------------------------------------------------


def _render_launcher_card(slug: str, icon: str, icon_color: str, title: str, desc: str, category: str) -> str:
    """Build the HTML for one launcher tile. Returns the HTML string so the
    caller can compose them inside the grid container.

    The card is wrapped in an <a> targeting the multi-page route so the
    entire tile is clickable. Streamlit serves multi-page apps at /<page_slug>
    relative to the app root.
    """
    return (
        f"<a class='nw-launcher-card' href='./{slug}' target='_self'>"
        "<div class='nw-launcher-head'>"
        f"<div class='nw-launcher-icon {icon_color}'>"
        f"<span class='material-symbols-outlined' style='font-size:28px;'>{icon}</span>"
        "</div>"
        f"<span class='nw-launcher-category'>{category}</span>"
        "</div>"
        f"<h3 class='nw-launcher-title'>{title}</h3>"
        f"<p class='nw-launcher-desc'>{desc}</p>"
        "<div class='nw-launcher-cta'>"
        "Open dashboard "
        "<span class='material-symbols-outlined' style='font-size:14px;'>arrow_forward</span>"
        "</div>"
        "</a>"
    )


def main():
    render_page_header(
        title="🎛️ Ops Command Center",
        subtitle="Unified analytics across sales, ops, marketing, and customer experience — one launcher for every Nowadays dashboard.",
    )

    # Status strip (replaces the legacy "live indicator" pulse)
    st.markdown(
        "<div class='nw-status-strip'>"
        "<div class='left'>"
        "<span class='live-dot'></span>"
        "All Systems Operational"
        "</div>"
        f"<div class='right'>{len(DASHBOARDS)} Dashboards</div>"
        "</div>",
        unsafe_allow_html=True,
    )

    # Launcher grid
    cards_html = "".join(_render_launcher_card(*d) for d in DASHBOARDS)
    st.markdown(
        f"<div class='nw-launcher-grid'>{cards_html}</div>",
        unsafe_allow_html=True,
    )

    render_footer("Nowadays internal analytics · zero-cost pipelines · powered by BigQuery + Streamlit Cloud")


if __name__ == "__main__":
    main()
