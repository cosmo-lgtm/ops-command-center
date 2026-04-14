"""
KAM Chain Performance Dashboard
SKU performance at micro and macro level across chain hierarchies.
Volume, SKU growth, and points of distribution by KAM portfolio.
"""

import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import plotly.express as px
import plotly.graph_objects as go
import numpy as np

from nowadays_ui import editorial_plotly, inject_editorial_style

# Page config
st.set_page_config(
    page_title="KAM Performance",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)
inject_editorial_style()

# Dark mode CSS



# ── BQ Connection ──────────────────────────────────────────────
@st.cache_resource
def get_bq_client():
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"]
    )
    return bigquery.Client(
        project='artful-logic-475116-p1',
        credentials=credentials
    )


# ── Data Loading ───────────────────────────────────────────────
@st.cache_data(ttl=300)
def load_door_data():
    client = get_bq_client()
    query = """
    SELECT
        vip_id, store_name, city, state, zip,
        chain_code, chain_name, class_of_trade_name, channel_type,
        primary_distributor_name,
        first_order_date, most_recent_order_date,
        days_since_last_order,
        qty_lifetime, qty_ytd, qty_last_30_days, qty_previous_30_days,
        qty_delta_30d, qty_pct_change_30d,
        qty_last_90_days, qty_previous_90_days,
        pod_lifetime, pod_ytd, pod_last_30, pod_previous_30, pod_delta,
        trend_30d, customer_status,
        sf_account_id, sf_account_name, sf_owner_name,
        google_latitude, google_longitude
    FROM `staging_vip.retail_customer_fact_sheet_2026`
    WHERE chain_code IS NOT NULL AND chain_code != ''
    """
    return client.query(query).to_dataframe()


@st.cache_data(ttl=300)
def load_chain_data():
    client = get_bq_client()
    return client.query("SELECT * FROM `staging_vip.chain_hq_fact_sheet_2026`").to_dataframe()


@st.cache_data(ttl=300)
def load_sku_data():
    client = get_bq_client()
    query = """
    SELECT
        chain_code, chain_name, vip_id, store_name, store_city, store_state,
        distributor_name, item_code, item_name,
        feb_25, mar_25, apr_25, may_25, jun_25, jul_25,
        aug_25, sep_25, oct_25, nov_25, dec_25, jan_26,
        ttm_current, ttm_prior, total_units_all_time
    FROM `staging_vip.chain_sales_report_2026`
    """
    return client.query(query).to_dataframe()


# ── Helpers ────────────────────────────────────────────────────
def format_number(n):
    if pd.isna(n):
        return "—"
    if abs(n) >= 1_000_000:
        return f"{n/1_000_000:,.1f}M"
    if abs(n) >= 1_000:
        return f"{n/1_000:,.1f}K"
    return f"{n:,.0f}"


def format_pct(n):
    if pd.isna(n) or n == float('inf') or n == float('-inf'):
        return "—"
    return f"{n:+.1f}%"


def render_metric(label, value, sublabel="", color="purple"):
    color_class = {
        "purple": "metric-value", "green": "metric-value-green",
        "gold": "metric-value-gold", "red": "metric-value-red",
    }.get(color, "metric-value")
    sub_html = f'<div class="metric-sublabel">{sublabel}</div>' if sublabel else ""
    st.markdown(f"""
    <div class="metric-card">
        <div class="{color_class}">{value}</div>
        <div class="metric-label">{label}</div>
        {sub_html}
    </div>
    """, unsafe_allow_html=True)


def apply_dark_theme(fig):
    return editorial_plotly(fig)


def get_volume_col(period):
    return {"Last 30d": "qty_last_30_days", "Last 90d": "qty_last_90_days",
            "YTD": "qty_ytd", "Lifetime": "qty_lifetime"}.get(period, "qty_last_30_days")


GRADIENT_COLORS = ['#667eea', '#764ba2', '#f093fb', '#f5576c', '#ffd700', '#64ffda', '#4facfe', '#00f2fe']

SCORECARD_CSS = """
<style>
.scorecard-wrap {
    font-family: 'Jost', sans-serif;
    background: #fef6ee;
    border-radius: 18px;
    padding: 32px 36px 24px 36px;
    border: 1.5px solid #e8ddd0;
    margin-bottom: 24px;
}
.scorecard-title {
    font-family: 'Jost', sans-serif;
    font-weight: 700;
    font-size: 1.6rem;
    color: #2D2926;
    margin-bottom: 4px;
}
.scorecard-title em {
    font-style: italic;
    font-weight: 400;
}
.scorecard-table {
    width: 100%;
    border-collapse: collapse;
    font-family: 'Jost', sans-serif;
    font-size: 0.92rem;
}
.scorecard-table thead th {
    background: #3d3530;
    color: #fff;
    padding: 10px 14px;
    text-align: center;
    font-weight: 600;
    font-size: 0.85rem;
    letter-spacing: 0.02em;
}
.scorecard-table thead th:first-child {
    text-align: left;
    border-radius: 8px 0 0 0;
}
.scorecard-table thead th:last-child {
    border-radius: 0 8px 0 0;
}
.scorecard-table tbody td {
    padding: 10px 14px;
    text-align: center;
    border-bottom: 1px solid #e8ddd0;
    color: #2D2926;
}
.scorecard-table tbody td:first-child {
    text-align: left;
    font-weight: 600;
}
.scorecard-table tbody td:nth-child(2) {
    font-size: 0.82rem;
    color: #7a6f63;
    font-style: italic;
}
.scorecard-table tbody tr:nth-child(even) {
    background: #f7efe4;
}
.scorecard-table tbody tr:nth-child(odd) {
    background: #fef9f1;
}
.scorecard-table .sc-pos { color: #2e7d32; font-weight: 600; }
.scorecard-table .sc-neg { color: #c62828; font-weight: 600; }
.scorecard-table .sc-neutral { color: #7a6f63; }
.scorecard-footer {
    text-align: right;
    margin-top: 10px;
    font-size: 1.2rem;
    opacity: 0.6;
}
</style>
"""


def compute_scorecard_kpis(doors, skus, chain_filter="All Chains"):
    """Compute all B2B scorecard KPIs with current vs prior period.

    All metrics derived from monthly SKU data (chain_sales_report) for accuracy.
    The door fact sheet's rolling period columns are unreliable for QoQ comparison.
    Door-level metadata (channel_type) still comes from the fact sheet.
    """
    if chain_filter != "All Chains":
        doors = doors[doors['chain_name'] == chain_filter].copy()
        skus = skus[skus['chain_name'] == chain_filter].copy()
    else:
        doors = doors.copy()
        skus = skus.copy()

    last_3 = ['nov_25', 'dec_25', 'jan_26']
    prior_3 = ['aug_25', 'sep_25', 'oct_25']

    if len(skus) > 0:
        skus['_l3m'] = skus[last_3].sum(axis=1)
        skus['_p3m'] = skus[prior_3].sum(axis=1)

        # 1. Depletions L3M vs Prior 3M (case equivalents) — from monthly SKU data
        depl_current = skus['_l3m'].sum()
        depl_prior = skus['_p3m'].sum()

        # YTD = all months in 2026 (jan_26 only currently)
        ytd_total = skus['jan_26'].sum() if 'jan_26' in skus.columns else 0

        # 2. Active Doors (distinct vip_ids with sales in period)
        active_current = skus[skus['_l3m'] > 0]['vip_id'].nunique()
        active_prior = skus[skus['_p3m'] > 0]['vip_id'].nunique()

        # 3. SKUs per Account (avg distinct items per active door)
        current_sku_per_door = (
            skus[skus['_l3m'] > 0]
            .groupby('vip_id')['item_code'].nunique()
        )
        skus_per_acct = current_sku_per_door.mean() if len(current_sku_per_door) > 0 else 0

        prior_sku_per_door = (
            skus[skus['_p3m'] > 0]
            .groupby('vip_id')['item_code'].nunique()
        )
        skus_per_acct_prior = prior_sku_per_door.mean() if len(prior_sku_per_door) > 0 else 0

        # 4. Velocity per Account (CE per active door)
        velocity = depl_current / active_current if active_current > 0 else 0
        velocity_prior = depl_prior / active_prior if active_prior > 0 else 0

        # 5. Off-Premise Buying % — join channel_type from door fact sheet
        off_prem_vids = set(
            doors[doors['channel_type'] == 'Off-Premise']['vip_id']
        )
        active_l3m_ids = set(skus[skus['_l3m'] > 0]['vip_id'])
        active_p3m_ids = set(skus[skus['_p3m'] > 0]['vip_id'])

        off_prem_current = len(active_l3m_ids & off_prem_vids)
        off_prem_prior = len(active_p3m_ids & off_prem_vids)
        off_prem_pct = off_prem_current / active_current * 100 if active_current > 0 else 0
        off_prem_pct_prior = off_prem_prior / active_prior * 100 if active_prior > 0 else 0

        # 6. Reorder Rate (doors ordering in 2+ of last 3 months)
        door_months = skus.groupby('vip_id')[last_3].sum()
        months_active = (door_months > 0).sum(axis=1)
        total_active_sku = len(months_active[months_active > 0])
        reorder_doors = len(months_active[months_active >= 2])
        reorder_rate = reorder_doors / total_active_sku * 100 if total_active_sku > 0 else 0

        door_months_prior = skus.groupby('vip_id')[prior_3].sum()
        months_active_prior = (door_months_prior > 0).sum(axis=1)
        total_active_prior = len(months_active_prior[months_active_prior > 0])
        reorder_prior = len(months_active_prior[months_active_prior >= 2])
        reorder_rate_prior = reorder_prior / total_active_prior * 100 if total_active_prior > 0 else 0
    else:
        depl_current = depl_prior = ytd_total = 0
        active_current = active_prior = 0
        skus_per_acct = skus_per_acct_prior = 0
        velocity = velocity_prior = 0
        off_prem_pct = off_prem_pct_prior = 0
        reorder_rate = reorder_rate_prior = 0

    kpis = [
        ("Depletions", "Case equivs (L3M)", depl_current, depl_prior, "", ",.0f"),
        ("Accounts Buying\n(Active Doors)", "# of active doors (L3M)", active_current, active_prior, "", ",.0f"),
        ("SKUs per Account", "Avg SKUs / door (L3M)", skus_per_acct, skus_per_acct_prior, "", ",.1f"),
        ("Velocity per Account", "Case equivs / door (L3M)", velocity, velocity_prior, "", ",.1f"),
        ("Off-Premise Buying %", "Off-Prem doors / Total (L3M)", off_prem_pct, off_prem_pct_prior, "%", ",.1f"),
        ("Reorder Rate", "Doors ordering 2x+ (L3M)", reorder_rate, reorder_rate_prior, "%", ",.1f"),
    ]
    return kpis, ytd_total


def render_scorecard_html(kpis):
    """Render the B2B Sales Scorecard as a styled HTML table."""
    rows = ""
    for name, uom, actual, prior, suffix, fmt in kpis:
        chg = actual - prior

        actual_str = f"{actual:{fmt}}{suffix}"
        prior_str = f"{prior:{fmt}}{suffix}"

        chg_class = "sc-pos" if chg > 0 else "sc-neg" if chg < 0 else "sc-neutral"
        chg_sign = "+" if chg > 0 else ""
        chg_str = f"{chg_sign}{chg:{fmt}}{suffix}"

        if prior and prior != 0:
            pct_chg = (actual - prior) / prior * 100
            pct_class = "sc-pos" if pct_chg > 0 else "sc-neg" if pct_chg < 0 else "sc-neutral"
            pct_sign = "+" if pct_chg > 0 else ""
            pct_str = f"{pct_sign}{pct_chg:,.1f}%"
        else:
            pct_class = "sc-pos" if actual > 0 else "sc-neutral"
            pct_str = "New" if actual > 0 else "—"

        display_name = name.replace("\n", "<br>")
        rows += f"""
        <tr>
            <td>{display_name}</td>
            <td>{uom}</td>
            <td>{actual_str}</td>
            <td>{prior_str}</td>
            <td class="{chg_class}">{chg_str}</td>
            <td class="{pct_class}">{pct_str}</td>
        </tr>"""

    return f"""
    <div class="scorecard-wrap">
        <div class="scorecard-title"><em>Nowadays</em> B2B Sales Scorecard — 2026</div>
        <table class="scorecard-table">
            <thead>
                <tr>
                    <th>KPI</th>
                    <th>UoM</th>
                    <th>Actuals</th>
                    <th>Prior Period</th>
                    <th>QoQ Chg</th>
                    <th>QoQ % Chg</th>
                </tr>
            </thead>
            <tbody>{rows}
            </tbody>
        </table>
        <div class="scorecard-footer">🌿</div>
    </div>"""


# ── Load Data ──────────────────────────────────────────────────
try:
    door_df = load_door_data()
    chain_df = load_chain_data()
    sku_df = load_sku_data()
except Exception as e:
    st.error(f"Failed to load data: {e}")
    st.stop()

# KAM → Chain mapping (majority owner of doors in chain)
kam_chain_map = (
    door_df.groupby(['chain_code', 'sf_owner_name'])
    .size().reset_index(name='door_count')
    .sort_values('door_count', ascending=False)
    .drop_duplicates(subset='chain_code', keep='first')
    [['chain_code', 'sf_owner_name']]
    .rename(columns={'sf_owner_name': 'kam'})
)
chain_df = chain_df.merge(kam_chain_map, on='chain_code', how='left')

# ── Header ─────────────────────────────────────────────────────
st.markdown('<div class="dashboard-header">KAM Chain Performance</div>', unsafe_allow_html=True)
st.markdown('<div class="dashboard-subtitle">SKU performance, volume trends & distribution growth across your chain portfolio</div>', unsafe_allow_html=True)

# ── Sidebar Filters ───────────────────────────────────────────
with st.sidebar:
    st.markdown("### Filters")
    all_kams = sorted(door_df['sf_owner_name'].dropna().unique().tolist())
    selected_kam = st.selectbox("Key Account Manager", ["All KAMs"] + all_kams, index=0)
    time_period = st.radio("Time Window", ["Last 30d", "Last 90d", "YTD", "Lifetime"], index=0)

    if selected_kam == "All KAMs":
        kam_chains = chain_df['chain_code'].dropna().unique().tolist()
        filtered_doors = door_df.copy()
    else:
        kam_chains = kam_chain_map[kam_chain_map['kam'] == selected_kam]['chain_code'].tolist()
        filtered_doors = door_df[door_df['chain_code'].isin(kam_chains)]

    available_chains = sorted(filtered_doors['chain_name'].dropna().unique().tolist())
    selected_chains = st.multiselect("Chains", available_chains, default=[])
    if selected_chains:
        filtered_doors = filtered_doors[filtered_doors['chain_name'].isin(selected_chains)]
        kam_chains = filtered_doors['chain_code'].dropna().unique().tolist()

    available_states = sorted(filtered_doors['state'].dropna().unique().tolist())
    selected_states = st.multiselect("States", available_states, default=[])
    if selected_states:
        filtered_doors = filtered_doors[filtered_doors['state'].isin(selected_states)]

    filtered_sku = sku_df[sku_df['chain_code'].isin(kam_chains)]
    if selected_states:
        filtered_sku = filtered_sku[filtered_sku['store_state'].isin(selected_states)]

    st.divider()
    st.caption(f"Showing {len(filtered_doors):,} doors across {filtered_doors['chain_code'].nunique()} chains")

vol_col = get_volume_col(time_period)

# ── TABS ───────────────────────────────────────────────────────
tab0, tab1, tab2, tab3, tab4 = st.tabs([
    "B2B Scorecard", "Portfolio Overview", "Chain Deep Dive", "SKU Performance", "Growth & Distribution"
])

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TAB 0: B2B SALES SCORECARD
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tab0:
    st.markdown(SCORECARD_CSS, unsafe_allow_html=True)

    # Inline chain filter for scorecard
    sc_chains = ["All Chains"] + sorted(filtered_doors['chain_name'].dropna().unique().tolist())
    sc_selected = st.selectbox("Chain", sc_chains, index=0, key="scorecard_chain")

    # Compute KPIs
    sc_kpis, ytd_val = compute_scorecard_kpis(filtered_doors, filtered_sku, sc_selected)

    # BANs — top 4 headline metrics
    b1, b2, b3, b4 = st.columns(4)
    active_val = sc_kpis[1][2]    # Active Doors (L90D)
    velocity_val = sc_kpis[3][2]  # Velocity per Account
    reorder_val = sc_kpis[5][2]   # Reorder Rate

    with b1:
        render_metric("YTD Depletions", format_number(ytd_val), "case equivalents", color="green")
    with b2:
        render_metric("Active Doors", format_number(active_val), "L3M", color="purple")
    with b3:
        render_metric("Velocity / Acct", f"{velocity_val:,.1f}", "CE per door (L3M)", color="gold")
    with b4:
        reorder_color = "green" if reorder_val >= 40 else "red" if reorder_val < 25 else "gold"
        render_metric("Reorder Rate", f"{reorder_val:,.1f}%", "2x+ orders (L3M)", color=reorder_color)

    # Scorecard table
    st.markdown(render_scorecard_html(sc_kpis), unsafe_allow_html=True)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TAB 1: PORTFOLIO OVERVIEW
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tab1:
    total_chains = filtered_doors['chain_code'].nunique()
    total_doors = len(filtered_doors)
    active_doors = len(filtered_doors[filtered_doors['customer_status'] == 'Active'])
    total_volume = filtered_doors[vol_col].sum()
    total_pod = filtered_doors['pod_last_30'].sum() if time_period == "Last 30d" else filtered_doors['pod_ytd'].sum()
    vol_current = filtered_doors['qty_last_30_days'].sum()
    vol_previous = filtered_doors['qty_previous_30_days'].sum()
    vol_delta_pct = ((vol_current - vol_previous) / vol_previous * 100) if vol_previous > 0 else 0

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    with c1: render_metric("Chains", format_number(total_chains))
    with c2: render_metric("Total Doors", format_number(total_doors), f"{active_doors:,} active")
    with c3: render_metric("Active Doors", format_number(active_doors), color="green")
    with c4: render_metric("Volume", format_number(total_volume), time_period, color="green" if total_volume > 0 else "red")
    with c5: render_metric("Total POD", format_number(total_pod), color="gold")
    with c6: render_metric("30d Growth", format_pct(vol_delta_pct), "vs prev 30d", color="green" if vol_delta_pct > 0 else "red" if vol_delta_pct < 0 else "purple")

    st.markdown('<div class="section-header">Chain Scorecard</div>', unsafe_allow_html=True)

    scorecard = (
        filtered_doors.groupby(['chain_code', 'chain_name'])
        .agg(
            total_stores=('vip_id', 'nunique'),
            active_stores=('customer_status', lambda x: (x == 'Active').sum()),
            at_risk_stores=('customer_status', lambda x: (x == 'At Risk').sum()),
            churned_stores=('customer_status', lambda x: (x == 'Churned').sum()),
            volume=(vol_col, 'sum'),
            qty_30d=('qty_last_30_days', 'sum'),
            qty_prev_30d=('qty_previous_30_days', 'sum'),
            pod_30=('pod_last_30', 'sum'),
            states=('state', 'nunique'),
        )
        .reset_index()
    )
    scorecard['delta_pct'] = scorecard.apply(
        lambda r: ((r['qty_30d'] - r['qty_prev_30d']) / r['qty_prev_30d'] * 100) if r['qty_prev_30d'] > 0 else 0, axis=1
    )
    scorecard['health'] = scorecard.apply(
        lambda r: 'Healthy' if r['active_stores'] >= r['total_stores'] * 0.7
        else ('At Risk' if r['active_stores'] >= r['total_stores'] * 0.4 else 'Critical'), axis=1
    )
    scorecard = scorecard.sort_values('volume', ascending=False)

    display_sc = scorecard[['chain_name', 'total_stores', 'active_stores', 'at_risk_stores',
                             'volume', 'delta_pct', 'pod_30', 'states', 'health']].copy()
    display_sc.columns = ['Chain', 'Stores', 'Active', 'At Risk', f'Volume ({time_period})',
                           '30d Δ%', 'POD (30d)', 'States', 'Health']
    display_sc[f'Volume ({time_period})'] = display_sc[f'Volume ({time_period})'].apply(lambda x: f"{x:,.0f}")
    display_sc['30d Δ%'] = display_sc['30d Δ%'].apply(lambda x: f"{x:+.1f}%")
    st.dataframe(display_sc, use_container_width=True, hide_index=True, height=400)

    st.markdown('<div class="section-header">Geographic Summary</div>', unsafe_allow_html=True)
    geo_col1, geo_col2 = st.columns(2)

    with geo_col1:
        state_vol = filtered_doors.groupby('state')[vol_col].sum().sort_values(ascending=False).head(15).reset_index()
        state_vol.columns = ['State', 'Volume']
        fig_state = px.bar(state_vol, x='Volume', y='State', orientation='h', title="Volume by State",
                           color='Volume', color_continuous_scale=['#667eea', '#764ba2'])
        fig_state.update_layout(showlegend=False, coloraxis_showscale=False)
        st.plotly_chart(apply_dark_theme(fig_state), use_container_width=True)

    with geo_col2:
        city_vol = (filtered_doors.groupby(['city', 'state'])
                    .agg(volume=(vol_col, 'sum'), doors=('vip_id', 'nunique'))
                    .sort_values('volume', ascending=False).head(15).reset_index())
        city_vol['location'] = city_vol['city'] + ', ' + city_vol['state']
        fig_city = px.bar(city_vol, x='volume', y='location', orientation='h', title="Top 15 Cities by Volume",
                          color='doors', color_continuous_scale=['#64ffda', '#667eea'],
                          labels={'volume': 'Volume', 'doors': 'Doors'})
        fig_city.update_layout(coloraxis_showscale=False)
        st.plotly_chart(apply_dark_theme(fig_city), use_container_width=True)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TAB 2: CHAIN DEEP DIVE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tab2:
    chain_names = sorted(filtered_doors['chain_name'].dropna().unique().tolist())
    if not chain_names:
        st.info("No chains available with current filters.")
    else:
        selected_chain = st.selectbox("Select Chain", chain_names, key="chain_deep_dive")
        chain_doors = filtered_doors[filtered_doors['chain_name'] == selected_chain]

        c1, c2, c3, c4, c5, c6 = st.columns(6)
        n_active = len(chain_doors[chain_doors['customer_status'] == 'Active'])
        n_risk = len(chain_doors[chain_doors['customer_status'] == 'At Risk'])
        n_churned = len(chain_doors[chain_doors['customer_status'] == 'Churned'])
        chain_vol = chain_doors[vol_col].sum()
        avg_vol = chain_vol / n_active if n_active > 0 else 0
        chain_30 = chain_doors['qty_last_30_days'].sum()
        chain_prev = chain_doors['qty_previous_30_days'].sum()
        chain_growth = ((chain_30 - chain_prev) / chain_prev * 100) if chain_prev > 0 else 0

        with c1: render_metric("Active", format_number(n_active), color="green")
        with c2: render_metric("At Risk", format_number(n_risk), color="gold")
        with c3: render_metric("Churned", format_number(n_churned), color="red")
        with c4: render_metric("Volume", format_number(chain_vol), time_period)
        with c5: render_metric("Avg/Store", format_number(avg_vol), "active stores", color="green")
        with c6:
            g_color = "green" if chain_growth > 0 else "red" if chain_growth < 0 else "purple"
            render_metric("30d Growth", format_pct(chain_growth), color=g_color)

        st.markdown('<div class="section-header">Store Performance</div>', unsafe_allow_html=True)
        store_display = chain_doors[[
            'store_name', 'city', 'state', 'customer_status', 'trend_30d',
            vol_col, 'pod_last_30', 'qty_delta_30d', 'qty_pct_change_30d', 'primary_distributor_name'
        ]].copy()
        store_display.columns = ['Store', 'City', 'State', 'Status', 'Trend',
                                  f'Volume ({time_period})', 'POD (30d)', 'Δ Units (30d)', 'Δ% (30d)', 'Distributor']
        store_display = store_display.sort_values(f'Volume ({time_period})', ascending=False)
        store_display[f'Volume ({time_period})'] = store_display[f'Volume ({time_period})'].apply(lambda x: f"{x:,.0f}" if pd.notna(x) else "—")
        store_display['Δ% (30d)'] = store_display['Δ% (30d)'].apply(lambda x: f"{x:+.1f}%" if pd.notna(x) and x != float('inf') else "—")
        st.dataframe(store_display, use_container_width=True, hide_index=True, height=400)

        map_col, city_col = st.columns(2)
        with map_col:
            map_data = chain_doors.dropna(subset=['google_latitude', 'google_longitude']).copy()
            if len(map_data) > 0:
                st.markdown('<div class="section-header">Store Map</div>', unsafe_allow_html=True)
                status_colors = {'Active': '#64ffda', 'At Risk': '#ffd700', 'Churned': '#ff6b6b'}
                map_data['color'] = map_data['customer_status'].map(status_colors).fillna('#8892b0')
                map_data['size'] = map_data[vol_col].clip(lower=1).apply(lambda x: max(np.log1p(x) * 3, 5))
                fig_map = px.scatter_map(
                    map_data, lat='google_latitude', lon='google_longitude',
                    size='size', color='customer_status', color_discrete_map=status_colors,
                    hover_name='store_name',
                    hover_data={'city': True, 'state': True, vol_col: True, 'size': False},
                    zoom=3, height=400,
                )
                fig_map.update_layout(map_style='carto-darkmatter', margin=dict(l=0, r=0, t=0, b=0))
                st.plotly_chart(apply_dark_theme(fig_map), use_container_width=True)
            else:
                st.info("No geocoded stores available for map view.")

        with city_col:
            st.markdown('<div class="section-header">Volume by City</div>', unsafe_allow_html=True)
            city_data = (chain_doors.groupby(['city', 'state'])
                         .agg(volume=(vol_col, 'sum'), doors=('vip_id', 'nunique'))
                         .sort_values('volume', ascending=False).head(15).reset_index())
            city_data['location'] = city_data['city'] + ', ' + city_data['state']
            fig_city_dd = px.bar(city_data, x='volume', y='location', orientation='h',
                                 color='doors', color_continuous_scale=['#64ffda', '#667eea'],
                                 labels={'volume': 'Volume', 'doors': 'Doors'}, height=400)
            fig_city_dd.update_layout(coloraxis_showscale=False)
            st.plotly_chart(apply_dark_theme(fig_city_dd), use_container_width=True)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TAB 3: SKU PERFORMANCE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tab3:
    sku_scope = st.radio("Scope", ["All Chains", "Single Chain"], horizontal=True, key="sku_scope")
    if sku_scope == "Single Chain":
        sku_chain = st.selectbox("Chain", chain_names if chain_names else ["—"], key="sku_chain_select")
        sku_working = filtered_sku[filtered_sku['chain_name'] == sku_chain] if sku_chain != "—" else filtered_sku
    else:
        sku_working = filtered_sku.copy()

    if len(sku_working) == 0:
        st.info("No SKU data available for current filters.")
    else:
        month_cols = ['feb_25', 'mar_25', 'apr_25', 'may_25', 'jun_25', 'jul_25',
                      'aug_25', 'sep_25', 'oct_25', 'nov_25', 'dec_25', 'jan_26']

        st.markdown('<div class="section-header">SKU Summary</div>', unsafe_allow_html=True)
        sku_summary = (
            sku_working.groupby(['item_code', 'item_name'])
            .agg(doors=('vip_id', 'nunique'), case_equiv_ttm=('ttm_current', 'sum'),
                 case_equiv_prior=('ttm_prior', 'sum'), raw_units=('total_units_all_time', 'sum'),
                 chains=('chain_code', 'nunique'), states=('store_state', 'nunique'))
            .reset_index().sort_values('case_equiv_ttm', ascending=False)
        )
        sku_summary['growth_pct'] = sku_summary.apply(
            lambda r: ((r['case_equiv_ttm'] - r['case_equiv_prior']) / r['case_equiv_prior'] * 100)
            if r['case_equiv_prior'] > 0 else 0, axis=1
        )

        display_sku = sku_summary[['item_name', 'doors', 'chains', 'states',
                                    'case_equiv_ttm', 'raw_units', 'growth_pct']].copy()
        display_sku.columns = ['SKU', 'Doors', 'Chains', 'States', 'Case Equiv (TTM)', 'Raw Units', 'YoY Δ%']
        display_sku['Case Equiv (TTM)'] = display_sku['Case Equiv (TTM)'].apply(lambda x: f"{x:,.1f}")
        display_sku['Raw Units'] = display_sku['Raw Units'].apply(lambda x: f"{x:,.0f}")
        display_sku['YoY Δ%'] = display_sku['YoY Δ%'].apply(lambda x: f"{x:+.1f}%")
        st.dataframe(display_sku, use_container_width=True, hide_index=True, height=350)

        heat_col, trend_col = st.columns(2)
        with heat_col:
            st.markdown('<div class="section-header">SKU × State Heatmap</div>', unsafe_allow_html=True)
            top_skus = sku_summary.head(10)['item_name'].tolist()
            heatmap_data = (sku_working[sku_working['item_name'].isin(top_skus)]
                            .groupby(['store_state', 'item_name'])['ttm_current'].sum().reset_index())
            if len(heatmap_data) > 0:
                pivot = heatmap_data.pivot_table(index='store_state', columns='item_name', values='ttm_current', fill_value=0)
                pivot['_total'] = pivot.sum(axis=1)
                pivot = pivot.sort_values('_total', ascending=False).head(20).drop(columns='_total')
                fig_heat = px.imshow(
                    pivot.values, labels=dict(x="SKU", y="State", color="Case Equiv"),
                    x=[n[:20] for n in pivot.columns], y=pivot.index.tolist(),
                    color_continuous_scale=['#0f0f1a', '#667eea', '#764ba2', '#f093fb'],
                    aspect='auto', height=500)
                fig_heat.update_layout(coloraxis_showscale=True)
                st.plotly_chart(apply_dark_theme(fig_heat), use_container_width=True)

        with trend_col:
            st.markdown('<div class="section-header">Monthly SKU Trend</div>', unsafe_allow_html=True)
            top_5_skus = sku_summary.head(5)['item_name'].tolist()
            monthly_by_sku = (sku_working[sku_working['item_name'].isin(top_5_skus)]
                              .groupby('item_name')[month_cols].sum())
            if len(monthly_by_sku) > 0:
                month_labels = ['Feb 25', 'Mar 25', 'Apr 25', 'May 25', 'Jun 25', 'Jul 25',
                                'Aug 25', 'Sep 25', 'Oct 25', 'Nov 25', 'Dec 25', 'Jan 26']
                fig_trend = go.Figure()
                for i, (sku_name, row) in enumerate(monthly_by_sku.iterrows()):
                    fig_trend.add_trace(go.Scatter(
                        x=month_labels, y=row.values, name=sku_name[:25], mode='lines+markers',
                        line=dict(color=GRADIENT_COLORS[i % len(GRADIENT_COLORS)], width=2),
                        marker=dict(size=6)))
                fig_trend.update_layout(title="Top 5 SKUs — Monthly Case Equivalents", height=500,
                                         legend=dict(orientation="h", yanchor="bottom", y=-0.3))
                st.plotly_chart(apply_dark_theme(fig_trend), use_container_width=True)

        st.markdown('<div class="section-header">SKU Movement (Last 30d)</div>', unsafe_allow_html=True)
        sku_movement = sku_working.groupby(['item_code', 'item_name']).agg(
            doors_current=('jan_26', lambda x: (x > 0).sum()),
            doors_previous=('dec_25', lambda x: (x > 0).sum()),
        ).reset_index()
        sku_movement['door_delta'] = sku_movement['doors_current'] - sku_movement['doors_previous']
        sku_movement = sku_movement[sku_movement['door_delta'] != 0].sort_values('door_delta', ascending=False)

        if len(sku_movement) > 0:
            gain_col, loss_col = st.columns(2)
            gained = sku_movement[sku_movement['door_delta'] > 0].head(10)
            lost = sku_movement[sku_movement['door_delta'] < 0].head(10)
            with gain_col:
                st.markdown("**SKUs Gaining Doors**")
                if len(gained) > 0:
                    g_display = gained[['item_name', 'doors_current', 'door_delta']].copy()
                    g_display.columns = ['SKU', 'Current Doors', 'Gained']
                    g_display['Gained'] = g_display['Gained'].apply(lambda x: f"+{x}")
                    st.dataframe(g_display, use_container_width=True, hide_index=True)
                else:
                    st.caption("No SKUs gained doors")
            with loss_col:
                st.markdown("**SKUs Losing Doors**")
                if len(lost) > 0:
                    l_display = lost[['item_name', 'doors_current', 'door_delta']].copy()
                    l_display.columns = ['SKU', 'Current Doors', 'Lost']
                    st.dataframe(l_display, use_container_width=True, hide_index=True)
                else:
                    st.caption("No SKUs lost doors")
        else:
            st.caption("No SKU movement detected between periods.")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TAB 4: GROWTH & DISTRIBUTION
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tab4:
    st.markdown('<div class="section-header">Distribution Breadth Over Time</div>', unsafe_allow_html=True)
    month_cols_list = ['feb_25', 'mar_25', 'apr_25', 'may_25', 'jun_25', 'jul_25',
                       'aug_25', 'sep_25', 'oct_25', 'nov_25', 'dec_25', 'jan_26']
    month_labels = ['Feb 25', 'Mar 25', 'Apr 25', 'May 25', 'Jun 25', 'Jul 25',
                    'Aug 25', 'Sep 25', 'Oct 25', 'Nov 25', 'Dec 25', 'Jan 26']

    pod_col, vol_trend_col = st.columns(2)
    with pod_col:
        pod_monthly = []
        for mc in month_cols_list:
            active_skus = filtered_sku[filtered_sku[mc] > 0]['item_code'].nunique()
            active_doors_m = filtered_sku[filtered_sku[mc] > 0]['vip_id'].nunique()
            pod_monthly.append({'skus': active_skus, 'doors': active_doors_m})
        pod_trend = pd.DataFrame(pod_monthly, index=month_labels)
        fig_pod = go.Figure()
        fig_pod.add_trace(go.Scatter(
            x=pod_trend.index, y=pod_trend['doors'], name='Active Doors', mode='lines+markers',
            line=dict(color='#64ffda', width=3), fill='tozeroy', fillcolor='rgba(100, 255, 218, 0.1)'))
        fig_pod.add_trace(go.Scatter(
            x=pod_trend.index, y=pod_trend['skus'], name='Distinct SKUs', mode='lines+markers',
            yaxis='y2', line=dict(color='#ffd700', width=3)))
        fig_pod.update_layout(
            title="Points of Distribution Trend",
            yaxis=dict(title="Active Doors", side="left"),
            yaxis2=dict(title="Distinct SKUs", side="right", overlaying="y"),
            height=400, legend=dict(orientation="h", yanchor="bottom", y=-0.2))
        st.plotly_chart(apply_dark_theme(fig_pod), use_container_width=True)

    with vol_trend_col:
        monthly_vol = filtered_sku[month_cols_list].sum()
        fig_vol_trend = go.Figure()
        fig_vol_trend.add_trace(go.Bar(
            x=month_labels, y=monthly_vol.values,
            marker=dict(color=monthly_vol.values, colorscale=[[0, '#667eea'], [1, '#764ba2']])))
        fig_vol_trend.update_layout(title="Monthly Volume (Case Equivalents)", height=400, showlegend=False)
        st.plotly_chart(apply_dark_theme(fig_vol_trend), use_container_width=True)

    st.markdown('<div class="section-header">Door Movement</div>', unsafe_allow_html=True)
    new_col, risk_col = st.columns(2)

    with new_col:
        st.markdown("**New / Reactivated Doors**")
        new_doors = filtered_doors[filtered_doors['trend_30d'].isin(['New', 'New Activity'])].copy()
        if len(new_doors) > 0:
            new_by_chain = (new_doors.groupby('chain_name')
                            .agg(new_doors_count=('vip_id', 'nunique'), volume=('qty_last_30_days', 'sum'))
                            .sort_values('new_doors_count', ascending=False).reset_index())
            new_by_chain.columns = ['Chain', 'New Doors', 'Volume (30d)']
            new_by_chain['Volume (30d)'] = new_by_chain['Volume (30d)'].apply(lambda x: f"{x:,.0f}")
            st.dataframe(new_by_chain, use_container_width=True, hide_index=True, height=300)
        else:
            st.caption("No new doors in period.")

    with risk_col:
        st.markdown("**At Risk / Churned Doors**")
        risk_doors = filtered_doors[filtered_doors['customer_status'].isin(['At Risk', 'Churned'])].copy()
        if len(risk_doors) > 0:
            risk_by_chain = (risk_doors.groupby(['chain_name', 'customer_status'])
                             .agg(count=('vip_id', 'nunique')).reset_index()
                             .pivot_table(index='chain_name', columns='customer_status', values='count', fill_value=0)
                             .reset_index())
            risk_by_chain.columns.name = None
            risk_by_chain = risk_by_chain.sort_values(risk_by_chain.columns[-1], ascending=False)
            st.dataframe(risk_by_chain, use_container_width=True, hide_index=True, height=300)
        else:
            st.caption("No at-risk or churned doors.")

    st.markdown('<div class="section-header">City Expansion</div>', unsafe_allow_html=True)
    city_growth = (
        filtered_doors.groupby(['city', 'state'])
        .agg(doors=('vip_id', 'nunique'),
             active=('customer_status', lambda x: (x == 'Active').sum()),
             growing=('trend_30d', lambda x: (x == 'Growing').sum()),
             new_doors=('trend_30d', lambda x: (x.isin(['New', 'New Activity'])).sum()),
             volume=('qty_last_30_days', 'sum'), prev_volume=('qty_previous_30_days', 'sum'))
        .reset_index()
    )
    city_growth['vol_delta'] = city_growth['volume'] - city_growth['prev_volume']
    city_growth['location'] = city_growth['city'] + ', ' + city_growth['state']
    top_growing = city_growth.sort_values('vol_delta', ascending=False).head(15)

    if len(top_growing) > 0:
        fig_expansion = px.bar(
            top_growing, x='vol_delta', y='location', orientation='h',
            title="Top Growing Cities (30d Volume Δ)", color='vol_delta',
            color_continuous_scale=['#ff6b6b', '#ffd700', '#64ffda'],
            labels={'vol_delta': 'Volume Δ'}, height=450)
        fig_expansion.update_layout(coloraxis_showscale=False)
        st.plotly_chart(apply_dark_theme(fig_expansion), use_container_width=True)
