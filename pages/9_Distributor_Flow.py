"""
Distributor Flow Intelligence Dashboard V2
Tracks product flow: Orders shipped → Retail depletion
Data source: staging_distro_metrics BigQuery views
"""

import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

# Page config
st.set_page_config(
    page_title="Distributor Flow",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Dark mode CSS (matching ops-command-center theme)
st.markdown("""
<style>
    .block-container {
        max-width: 100% !important;
        padding-left: 2rem !important;
        padding-right: 2rem !important;
    }
    .stApp {
        background: linear-gradient(135deg, #0f0f1a 0%, #1a1a2e 50%, #16213e 100%);
    }
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden; height: 0px !important;}
    .stApp > header {display: none !important;}
    .stDeployButton {display: none !important;}
    [data-testid="stHeader"] {display: none !important;}
    [data-testid="stToolbar"] {display: none !important;}
    .block-container {padding-top: 1rem !important;}
</style>
""", unsafe_allow_html=True)

# BigQuery connection
@st.cache_resource
def get_bq_client():
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"]
    )
    return bigquery.Client(credentials=credentials, project="artful-logic-475116-p1")

@st.cache_data(ttl=300)
def run_query(query: str) -> pd.DataFrame:
    client = get_bq_client()
    return client.query(query).to_dataframe()

# Sidebar - cache control
with st.sidebar:
    if st.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()

# Data loading
@st.cache_data(ttl=300)
def load_flow_data():
    return run_query("""
    SELECT *
    FROM `artful-logic-475116-p1.staging_distro_metrics.fct_distributor_flow_by_parent`
    ORDER BY order_value DESC
    """)

@st.cache_data(ttl=300)
def load_replenishment_signals():
    return run_query("""
    SELECT *
    FROM `artful-logic-475116-p1.staging_distro_metrics.fct_replenishment_signals`
    ORDER BY priority ASC, units_depleted_90d DESC
    """)

@st.cache_data(ttl=300)
def load_weekly_trend():
    return run_query("""
    WITH weekly_orders AS (
      SELECT
        DATE_TRUNC(order_date, WEEK) as week_start,
        SUM(CAST(quantity AS INT64) *
          CASE
            WHEN product_name LIKE '%24 cans%' THEN 24
            WHEN product_name LIKE '%6 bottles%' THEN 6
            WHEN product_name LIKE '%36 bottles%' THEN 36
            ELSE 1
          END
        ) as units_ordered,
        ROUND(SUM(CAST(line_total_price AS FLOAT64)), 0) as order_value
      FROM `artful-logic-475116-p1.staging_salesforce.salesforce_orders_flattened`
      WHERE account_type = 'Distributor'
        AND order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 WEEK)
      GROUP BY week_start
    ),
    weekly_depletion AS (
      SELECT
        DATE_TRUNC(SAFE.PARSE_DATE('%Y%m%d', Invoice_Date), WEEK) as week_start,
        SUM(SAFE_CAST(Qty AS INT64)) as units_depleted
      FROM `artful-logic-475116-p1.raw_vip.sales_lite`
      WHERE SAFE_CAST(Qty AS INT64) > 0
        AND SAFE.PARSE_DATE('%Y%m%d', Invoice_Date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 WEEK)
      GROUP BY week_start
    )
    SELECT
      COALESCE(wo.week_start, wd.week_start) as week_start,
      COALESCE(wo.units_ordered, 0) as units_ordered,
      COALESCE(wo.order_value, 0) as order_value,
      COALESCE(wd.units_depleted, 0) as units_depleted
    FROM weekly_orders wo
    FULL OUTER JOIN weekly_depletion wd ON wo.week_start = wd.week_start
    WHERE COALESCE(wo.week_start, wd.week_start) IS NOT NULL
    ORDER BY week_start
    """)

@st.cache_data(ttl=300)
def load_data_quality():
    return run_query("""
    SELECT
      (SELECT MAX(order_date) FROM `artful-logic-475116-p1.staging_salesforce.salesforce_orders_flattened`
       WHERE account_type = 'Distributor') as sf_latest_date,
      (SELECT MAX(SAFE.PARSE_DATE('%Y%m%d', Invoice_Date)) FROM `artful-logic-475116-p1.raw_vip.sales_lite`
       WHERE SAFE_CAST(Qty AS INT64) > 0) as vip_latest_date,
      (SELECT COUNT(DISTINCT account_id) FROM `artful-logic-475116-p1.staging_salesforce.salesforce_orders_flattened`
       WHERE account_type = 'Distributor' AND order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) as sf_distributor_count,
      (SELECT COUNT(*) FROM `artful-logic-475116-p1.staging_distro_metrics.dim_distributor`) as vip_distributor_count,
      (SELECT COUNT(*) FROM `artful-logic-475116-p1.staging_distro_metrics.fct_distributor_flow_by_parent` WHERE has_sf_orders AND has_vip_data) as matched_count
    """)

# Formatting
def format_currency(val):
    return f"${val:,.0f}" if pd.notna(val) else "-"

def format_number(val):
    return f"{val:,.0f}" if pd.notna(val) else "-"

# Main
st.title("📊 Distributor Flow Intelligence")
st.caption("Tracking product flow: Orders shipped → Retail depletion (90-day rolling)")

try:
    flow_df = load_flow_data()
    signals_df = load_replenishment_signals()
    trend_df = load_weekly_trend()
    quality_df = load_data_quality()
except Exception as e:
    st.error(f"Error loading data: {e}")
    st.stop()

# Tabs
tab1, tab2, tab3, tab4 = st.tabs([
    "📈 Overview",
    "🏢 Distributors",
    "🎯 Opportunities",
    "📋 Data Quality"
])

# TAB 1: OVERVIEW
with tab1:
    # KPIs
    col1, col2, col3, col4, col5 = st.columns(5)
    total_order_value = flow_df['order_value'].sum()
    total_units_ordered = flow_df['units_ordered'].sum()
    total_units_depleted = flow_df['units_depleted'].sum()
    flow_ratio = total_units_ordered / total_units_depleted if total_units_depleted > 0 else 0
    active_distributors = len(flow_df[flow_df['has_sf_orders']])

    col1.metric("Order Value (90d)", format_currency(total_order_value))
    col2.metric("Units Ordered", format_number(total_units_ordered))
    col3.metric("Units Depleted", format_number(total_units_depleted))
    col4.metric("Flow Ratio", f"{flow_ratio:.2f}x", help=">1 = building inventory")
    col5.metric("Active Distributors", active_distributors)

    st.divider()

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Weekly Flow Trend")
        if not trend_df.empty:
            fig = go.Figure()
            fig.add_trace(go.Bar(x=trend_df['week_start'], y=trend_df['units_ordered'],
                                 name='Units Ordered', marker_color='#2E86AB'))
            fig.add_trace(go.Scatter(x=trend_df['week_start'], y=trend_df['units_depleted'],
                                     name='Units Depleted', line=dict(color='#E94F37', width=3)))
            fig.update_layout(barmode='group', height=350, margin=dict(l=20, r=20, t=20, b=20),
                              legend=dict(orientation="h", yanchor="bottom", y=1.02),
                              paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
                              font=dict(color='#ccd6f6'))
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Flow Status Distribution")
        status_counts = flow_df['flow_status'].value_counts()
        color_map = {
            'Building Fast': '#2E86AB', 'Building': '#7FB069', 'Balanced': '#F4D35E',
            'Depleting': '#EE964B', 'Depleting Fast': '#E94F37',
            'VIP Only (No Orders)': '#95A5A6', 'SF Only (No VIP)': '#BDC3C7'
        }
        fig = px.pie(values=status_counts.values, names=status_counts.index,
                     color=status_counts.index, color_discrete_map=color_map, hole=0.4)
        fig.update_layout(height=350, margin=dict(l=20, r=20, t=20, b=20),
                          paper_bgcolor='rgba(0,0,0,0)', font=dict(color='#ccd6f6'))
        st.plotly_chart(fig, use_container_width=True)

    # Top distributors
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("🏆 Top 5 by Order Value")
        top_5 = flow_df.nlargest(5, 'order_value')[['parent_name', 'order_value', 'units_ordered', 'flow_status']].copy()
        top_5['order_value'] = top_5['order_value'].apply(format_currency)
        top_5['units_ordered'] = top_5['units_ordered'].apply(format_number)
        top_5.columns = ['Distributor', 'Order Value', 'Units', 'Status']
        st.dataframe(top_5, hide_index=True, use_container_width=True)

    with col2:
        st.subheader("⚠️ High Depletion, Low Orders")
        concern = flow_df[(flow_df['units_depleted'] > 5000) & (flow_df['flow_ratio'].fillna(0) < 0.5)].nsmallest(5, 'flow_ratio')[['parent_name', 'units_depleted', 'units_ordered', 'flow_status']].copy()
        if not concern.empty:
            concern['units_depleted'] = concern['units_depleted'].apply(format_number)
            concern['units_ordered'] = concern['units_ordered'].apply(format_number)
            concern.columns = ['Distributor', 'Depleted', 'Ordered', 'Status']
            st.dataframe(concern, hide_index=True, use_container_width=True)
        else:
            st.success("No distributors of concern")

# TAB 2: DISTRIBUTORS
with tab2:
    col1, col2, col3 = st.columns(3)
    with col1:
        status_filter = st.multiselect("Flow Status", options=flow_df['flow_status'].unique().tolist(), default=[])
    with col2:
        min_order_value = st.number_input("Min Order Value ($)", value=0, step=10000)
    with col3:
        show_vip_only = st.checkbox("Include VIP-only", value=True)

    filtered_df = flow_df.copy()
    if status_filter:
        filtered_df = filtered_df[filtered_df['flow_status'].isin(status_filter)]
    if min_order_value > 0:
        filtered_df = filtered_df[filtered_df['order_value'] >= min_order_value]
    if not show_vip_only:
        filtered_df = filtered_df[filtered_df['has_sf_orders']]

    # Scatter
    scatter_df = filtered_df[(filtered_df['units_ordered'] > 0) | (filtered_df['units_depleted'] > 0)].copy()
    if not scatter_df.empty:
        fig = px.scatter(scatter_df, x='units_depleted', y='units_ordered', size='order_value',
                         color='flow_status', hover_name='parent_name',
                         color_discrete_map={'Building Fast': '#2E86AB', 'Building': '#7FB069',
                                             'Balanced': '#F4D35E', 'Depleting': '#EE964B',
                                             'Depleting Fast': '#E94F37', 'VIP Only (No Orders)': '#95A5A6',
                                             'SF Only (No VIP)': '#BDC3C7'},
                         labels={'units_depleted': 'Units Depleted', 'units_ordered': 'Units Ordered'})
        max_val = max(scatter_df['units_ordered'].max(), scatter_df['units_depleted'].max())
        fig.add_trace(go.Scatter(x=[0, max_val], y=[0, max_val], mode='lines',
                                 line=dict(color='gray', dash='dash'), name='1:1 Ratio'))
        fig.update_layout(height=450, paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
                          font=dict(color='#ccd6f6'))
        st.plotly_chart(fig, use_container_width=True)

    # Table
    display_df = filtered_df[['parent_name', 'order_value', 'units_ordered', 'units_depleted',
                              'flow_ratio', 'stores_reached', 'order_count', 'flow_status']].copy()
    display_df.columns = ['Distributor', 'Order Value', 'Units Ordered', 'Units Depleted',
                          'Flow Ratio', 'Stores', 'Orders', 'Status']
    display_df['Order Value'] = display_df['Order Value'].apply(format_currency)
    display_df['Units Ordered'] = display_df['Units Ordered'].apply(format_number)
    display_df['Units Depleted'] = display_df['Units Depleted'].apply(format_number)
    display_df['Flow Ratio'] = display_df['Flow Ratio'].apply(lambda x: f"{x:.2f}" if pd.notna(x) else "-")
    display_df['Stores'] = display_df['Stores'].apply(format_number)
    st.dataframe(display_df, hide_index=True, use_container_width=True, height=400)
    st.caption(f"Showing {len(filtered_df)} distributors")

# TAB 3: OPPORTUNITIES
with tab3:
    signal_counts = signals_df['signal_type'].value_counts()
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("🎯 True Opportunities", signal_counts.get('True Opportunity - Never Ordered', 0))
    col2.metric("🔄 Needs Reorder", signal_counts.get('Needs Reorder - High Velocity', 0))
    col3.metric("⚠️ Lapsed", signal_counts.get('Lapsed - Over 90 Days', 0))
    col4.metric("✅ Active", signal_counts.get('Active', 0))

    st.divider()

    signal_filter = st.multiselect("Filter Signals", options=signals_df['signal_type'].unique().tolist(),
                                   default=['True Opportunity - Never Ordered', 'Needs Reorder - High Velocity', 'Lapsed - Over 90 Days'])

    display_signals = signals_df[signals_df['signal_type'].isin(signal_filter)] if signal_filter else signals_df

    st.subheader("Priority Actions")
    priority_df = display_signals[display_signals['priority'] <= 2][['distributor_name', 'parent_name', 'signal_type',
                                                                      'units_depleted_90d', 'weekly_rate', 'stores_reached', 'days_since_order']].copy()
    if not priority_df.empty:
        priority_df.columns = ['Distributor', 'Parent', 'Signal', 'Units (90d)', 'Weekly Rate', 'Stores', 'Days Since Order']
        priority_df['Units (90d)'] = priority_df['Units (90d)'].apply(format_number)
        priority_df['Weekly Rate'] = priority_df['Weekly Rate'].apply(format_number)
        priority_df['Stores'] = priority_df['Stores'].apply(format_number)
        priority_df['Days Since Order'] = priority_df['Days Since Order'].apply(lambda x: f"{int(x)} days" if pd.notna(x) else "Never")
        st.dataframe(priority_df, hide_index=True, use_container_width=True)
    else:
        st.success("No priority actions needed!")

    # Chart
    opps_df = signals_df[signals_df['signal_type'] != 'Active'].nlargest(15, 'units_depleted_90d')
    if not opps_df.empty:
        fig = px.bar(opps_df, x='units_depleted_90d', y='distributor_name', color='signal_type', orientation='h',
                     color_discrete_map={'True Opportunity - Never Ordered': '#E94F37',
                                         'Needs Reorder - High Velocity': '#EE964B', 'Lapsed - Over 90 Days': '#F4D35E'},
                     labels={'units_depleted_90d': 'Units Depleted (90d)', 'distributor_name': ''})
        fig.update_layout(height=400, yaxis={'categoryorder': 'total ascending'},
                          paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', font=dict(color='#ccd6f6'),
                          legend=dict(orientation="h", yanchor="bottom", y=1.02))
        st.plotly_chart(fig, use_container_width=True)

# TAB 4: DATA QUALITY
with tab4:
    if not quality_df.empty:
        row = quality_df.iloc[0]

        st.subheader("Data Freshness")
        col1, col2 = st.columns(2)
        with col1:
            sf_date = row['sf_latest_date']
            sf_lag = (datetime.now().date() - sf_date).days if sf_date else None
            status = "🟢" if sf_lag and sf_lag <= 1 else "🟡" if sf_lag and sf_lag <= 3 else "🔴"
            st.metric(f"{status} Salesforce Orders", sf_date.strftime('%Y-%m-%d') if sf_date else "N/A",
                      f"{sf_lag} days ago" if sf_lag else None)
        with col2:
            vip_date = row['vip_latest_date']
            vip_lag = (datetime.now().date() - vip_date).days if vip_date else None
            status = "🟢" if vip_lag and vip_lag <= 3 else "🟡" if vip_lag and vip_lag <= 7 else "🔴"
            st.metric(f"{status} VIP Depletion", vip_date.strftime('%Y-%m-%d') if vip_date else "N/A",
                      f"{vip_lag} days ago" if vip_lag else None)

        st.divider()
        st.subheader("Distributor Coverage")
        col1, col2, col3 = st.columns(3)
        col1.metric("SF Distributors (90d)", row['sf_distributor_count'])
        col2.metric("VIP Distributors", row['vip_distributor_count'])
        match_rate = (row['matched_count'] / row['sf_distributor_count'] * 100) if row['sf_distributor_count'] else 0
        col3.metric("Matched (both)", f"{row['matched_count']} ({match_rate:.0f}%)")

    st.divider()
    st.markdown("""
    **What this shows:** Orders (SF) vs Depletion (VIP) | Flow Ratio >1 = building inventory
    **Limitations:** No actual inventory data (VIP inventory not in SFTP) | ~21% SF distributors not yet in VIP
    """)

st.caption(f"Last refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M')} | Data: staging_distro_metrics")
