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

# Page config
st.set_page_config(
    page_title="KAM Performance",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Dark mode CSS
st.markdown("""
<style>
    .stApp {
        background: linear-gradient(135deg, #0f0f1a 0%, #1a1a2e 50%, #16213e 100%);
    }
    .block-container {
        max-width: 100% !important;
        padding-left: 2rem !important;
        padding-right: 2rem !important;
    }
    [data-testid="stHorizontalBlock"] {
        flex-wrap: nowrap !important;
        gap: 1rem;
    }
    [data-testid="stColumn"] {
        min-width: 0 !important;
        flex: 1 1 0 !important;
    }

    .metric-card {
        background: linear-gradient(145deg, #1e1e2f 0%, #2a2a4a 100%);
        border-radius: 16px;
        padding: 24px;
        border: 1px solid rgba(255,255,255,0.1);
        box-shadow: 0 8px 32px rgba(0,0,0,0.3);
        transition: transform 0.3s ease, box-shadow 0.3s ease;
        margin-bottom: 16px;
    }
    .metric-card:hover {
        transform: translateY(-4px);
        box-shadow: 0 12px 40px rgba(0,0,0,0.4);
    }
    .metric-value {
        font-size: 36px;
        font-weight: 700;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin: 0;
    }
    .metric-value-green {
        font-size: 36px;
        font-weight: 700;
        background: linear-gradient(135deg, #64ffda 0%, #00bfa5 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin: 0;
    }
    .metric-value-gold {
        font-size: 36px;
        font-weight: 700;
        background: linear-gradient(135deg, #ffd700 0%, #ff8c00 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin: 0;
    }
    .metric-value-red {
        font-size: 36px;
        font-weight: 700;
        background: linear-gradient(135deg, #ff6b6b 0%, #ee5a24 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin: 0;
    }
    .metric-label {
        font-size: 14px;
        color: #8892b0;
        text-transform: uppercase;
        letter-spacing: 1.5px;
        margin-top: 8px;
    }
    .metric-sublabel {
        font-size: 12px;
        color: #5a6785;
        margin-top: 4px;
    }
    .dashboard-header {
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-size: 42px;
        font-weight: 800;
        margin-bottom: 8px;
    }
    .dashboard-subtitle {
        color: #8892b0;
        font-size: 16px;
        margin-bottom: 32px;
    }
    .section-header {
        color: #ccd6f6;
        font-size: 22px;
        font-weight: 600;
        margin: 28px 0 16px 0;
        padding-bottom: 8px;
        border-bottom: 2px solid rgba(102, 126, 234, 0.3);
    }
    .status-healthy {
        background: linear-gradient(135deg, #64ffda 0%, #00bfa5 100%);
        color: #0f0f1a; padding: 4px 12px; border-radius: 20px; font-size: 11px; font-weight: 700;
    }
    .status-warning {
        background: linear-gradient(135deg, #ffd700 0%, #ff8c00 100%);
        color: #0f0f1a; padding: 4px 12px; border-radius: 20px; font-size: 11px; font-weight: 700;
    }
    .status-critical {
        background: linear-gradient(135deg, #ff6b6b 0%, #ee5a24 100%);
        color: #0f0f1a; padding: 4px 12px; border-radius: 20px; font-size: 11px; font-weight: 700;
    }
    [data-testid="stDataFrame"] {
        background: rgba(30, 30, 47, 0.6);
        border-radius: 12px;
        border: 1px solid rgba(255,255,255,0.08);
    }
    .stTabs [data-baseweb="tab-list"] { gap: 8px; }
    .stTabs [data-baseweb="tab"] {
        background: rgba(30, 30, 47, 0.6); border-radius: 8px; color: #8892b0; padding: 8px 16px;
    }
    .stTabs [aria-selected="true"] {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white;
    }
</style>
""", unsafe_allow_html=True)


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
    fig.update_layout(
        paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
        font_color='#ccd6f6', title_font_color='#ccd6f6', legend_font_color='#8892b0',
        margin=dict(l=20, r=20, t=40, b=20),
    )
    fig.update_xaxes(gridcolor='rgba(255,255,255,0.05)', zerolinecolor='rgba(255,255,255,0.1)')
    fig.update_yaxes(gridcolor='rgba(255,255,255,0.05)', zerolinecolor='rgba(255,255,255,0.1)')
    return fig


def get_volume_col(period):
    return {"Last 30d": "qty_last_30_days", "Last 90d": "qty_last_90_days",
            "YTD": "qty_ytd", "Lifetime": "qty_lifetime"}.get(period, "qty_last_30_days")


GRADIENT_COLORS = ['#667eea', '#764ba2', '#f093fb', '#f5576c', '#ffd700', '#64ffda', '#4facfe', '#00f2fe']


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
tab1, tab2, tab3, tab4 = st.tabs([
    "Portfolio Overview", "Chain Deep Dive", "SKU Performance", "Growth & Distribution"
])

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
