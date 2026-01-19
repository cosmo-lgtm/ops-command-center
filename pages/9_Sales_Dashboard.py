"""
Combined Sales Dashboard - B2B + B2C
Tracks all revenue: Salesforce (B2B) + Shopify (B2C) with seasonal forecasting.
Replaces the old Distributor Flow dashboard.
"""

import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import numpy as np

# Page config
st.set_page_config(
    page_title="Sales Dashboard",
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

    /* KPI Cards */
    .kpi-card {
        background: linear-gradient(145deg, #1e1e2f 0%, #2a2a4a 100%);
        border-radius: 12px;
        padding: 20px;
        border: 1px solid rgba(255,255,255,0.1);
        text-align: center;
    }
    .kpi-value {
        font-size: 32px;
        font-weight: 700;
        color: #00d4aa;
        margin: 0;
    }
    .kpi-label {
        font-size: 12px;
        color: #8892b0;
        text-transform: uppercase;
        letter-spacing: 1px;
        margin-top: 4px;
    }
    .kpi-delta-positive {
        color: #64ffda;
        font-size: 12px;
    }
    .kpi-delta-negative {
        color: #ff6b6b;
        font-size: 12px;
    }
</style>
""", unsafe_allow_html=True)

COLORS = {
    'b2b': '#2E86AB',      # Blue for B2B
    'b2c': '#00d4aa',      # Teal for B2C
    'forecast': '#ffd666', # Yellow for forecast
    'total': '#667eea',    # Purple for total
}


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


# ============================================================================
# DATA LOADING FUNCTIONS
# ============================================================================

@st.cache_data(ttl=300)
def load_b2b_daily(lookback_days: int = 90):
    """Load B2B (Salesforce) daily sales - all non-draft orders."""
    return run_query(f"""
    SELECT
        sfo.order_date,
        COUNT(DISTINCT sfo.order_id) as order_count,
        ROUND(SUM(CAST(sfo.line_total_price AS FLOAT64)), 2) as revenue,
        SUM(CAST(sfo.quantity AS FLOAT64)) as units,
        EXTRACT(DAYOFWEEK FROM sfo.order_date) as day_of_week
    FROM `artful-logic-475116-p1.staging_salesforce.salesforce_orders_flattened` sfo
    WHERE sfo.order_status != 'Draft'
        AND sfo.order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL {lookback_days} DAY)
        AND sfo.order_date <= CURRENT_DATE()
    GROUP BY sfo.order_date, day_of_week
    ORDER BY sfo.order_date
    """)


@st.cache_data(ttl=300)
def load_b2b_by_account(lookback_days: int = 90):
    """Load B2B sales by account with owner info."""
    return run_query(f"""
    SELECT
        sfo.account_id,
        sfo.customer_name,
        sfo.account_type,
        u.Name as owner_name,
        COUNT(DISTINCT sfo.order_id) as order_count,
        ROUND(SUM(CAST(sfo.line_total_price AS FLOAT64)), 2) as revenue,
        SUM(CAST(sfo.quantity AS FLOAT64)) as units,
        MAX(sfo.order_date) as last_order_date,
        MIN(sfo.order_date) as first_order_date
    FROM `artful-logic-475116-p1.staging_salesforce.salesforce_orders_flattened` sfo
    LEFT JOIN `artful-logic-475116-p1.raw_salesforce.Account` a ON sfo.account_id = a.Id
    LEFT JOIN `artful-logic-475116-p1.raw_salesforce.User` u ON a.OwnerId = u.Id
    WHERE sfo.order_status != 'Draft'
        AND sfo.order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL {lookback_days} DAY)
        AND sfo.order_date <= CURRENT_DATE()
    GROUP BY sfo.account_id, sfo.customer_name, sfo.account_type, u.Name
    ORDER BY revenue DESC
    """)


@st.cache_data(ttl=300)
def load_b2b_weekly(lookback_days: int = 90):
    """Load B2B weekly aggregation."""
    return run_query(f"""
    SELECT
        DATE_TRUNC(order_date, WEEK(MONDAY)) as week_start,
        COUNT(DISTINCT order_id) as order_count,
        ROUND(SUM(CAST(line_total_price AS FLOAT64)), 2) as revenue,
        SUM(CAST(quantity AS FLOAT64)) as units
    FROM `artful-logic-475116-p1.staging_salesforce.salesforce_orders_flattened`
    WHERE order_status != 'Draft'
        AND order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL {lookback_days} DAY)
        AND order_date <= CURRENT_DATE()
    GROUP BY week_start
    ORDER BY week_start
    """)


@st.cache_data(ttl=300)
def load_b2c_daily(lookback_days: int = 90):
    """Load B2C (Shopify) daily sales."""
    return run_query(f"""
    SELECT
        DATE(created_at) as order_date,
        COUNT(DISTINCT id) as order_count,
        ROUND(SUM(CAST(total_price AS FLOAT64)), 2) as revenue,
        EXTRACT(DAYOFWEEK FROM created_at) as day_of_week
    FROM `artful-logic-475116-p1.raw_shopify.orders`
    WHERE cancelled_at IS NULL
        AND financial_status IN ('paid', 'partially_refunded')
        AND created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {lookback_days} DAY)
        AND created_at <= CURRENT_TIMESTAMP()
    GROUP BY order_date, day_of_week
    ORDER BY order_date
    """)


@st.cache_data(ttl=300)
def load_b2c_weekly(lookback_days: int = 90):
    """Load B2C weekly aggregation."""
    return run_query(f"""
    SELECT
        DATE_TRUNC(DATE(created_at), WEEK(MONDAY)) as week_start,
        COUNT(DISTINCT id) as order_count,
        ROUND(SUM(CAST(total_price AS FLOAT64)), 2) as revenue
    FROM `artful-logic-475116-p1.raw_shopify.orders`
    WHERE cancelled_at IS NULL
        AND financial_status IN ('paid', 'partially_refunded')
        AND created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {lookback_days} DAY)
        AND created_at <= CURRENT_TIMESTAMP()
    GROUP BY week_start
    ORDER BY week_start
    """)


@st.cache_data(ttl=300)
def load_b2c_products(lookback_days: int = 90):
    """Load B2C product performance."""
    return run_query(f"""
    WITH order_items AS (
        SELECT
            o.id as order_id,
            DATE(o.created_at) as order_date,
            JSON_VALUE(item, '$.title') as product_name,
            JSON_VALUE(item, '$.sku') as sku,
            CAST(JSON_VALUE(item, '$.quantity') AS INT64) as quantity,
            CAST(JSON_VALUE(item, '$.price') AS FLOAT64) as unit_price
        FROM `artful-logic-475116-p1.raw_shopify.orders` o,
        UNNEST(JSON_QUERY_ARRAY(o.line_items)) as item
        WHERE o.cancelled_at IS NULL
            AND o.financial_status IN ('paid', 'partially_refunded')
            AND o.created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {lookback_days} DAY)
    )
    SELECT
        product_name,
        sku,
        SUM(quantity) as units_sold,
        ROUND(SUM(quantity * unit_price), 2) as revenue,
        COUNT(DISTINCT order_id) as order_count,
        ROUND(AVG(unit_price), 2) as avg_price
    FROM order_items
    WHERE product_name IS NOT NULL
        AND product_name NOT LIKE '%Shipping Protection%'
        AND product_name NOT LIKE '%Protectly%'
    GROUP BY product_name, sku
    HAVING units_sold > 0
    ORDER BY revenue DESC
    """)


@st.cache_data(ttl=300)
def load_filter_options():
    """Load distinct values for filters."""
    owners = run_query("""
    SELECT DISTINCT u.Name as owner_name
    FROM `artful-logic-475116-p1.staging_salesforce.salesforce_orders_flattened` sfo
    LEFT JOIN `artful-logic-475116-p1.raw_salesforce.Account` a ON sfo.account_id = a.Id
    LEFT JOIN `artful-logic-475116-p1.raw_salesforce.User` u ON a.OwnerId = u.Id
    WHERE sfo.order_status != 'Draft'
        AND u.Name IS NOT NULL
    ORDER BY u.Name
    """)

    account_types = run_query("""
    SELECT DISTINCT account_type
    FROM `artful-logic-475116-p1.staging_salesforce.salesforce_orders_flattened`
    WHERE order_status != 'Draft'
        AND account_type IS NOT NULL
    ORDER BY account_type
    """)

    return owners['owner_name'].tolist(), account_types['account_type'].tolist()


# ============================================================================
# FORECASTING
# ============================================================================

def calculate_seasonal_forecast(daily_df: pd.DataFrame, forecast_days: int = 30):
    """Calculate seasonal forecast with day-of-week factors."""
    if daily_df.empty or len(daily_df) < 14:
        return pd.DataFrame()

    df = daily_df.sort_values('order_date').copy()

    # 7-day moving average
    df['ma_7'] = df['revenue'].rolling(window=7, min_periods=1).mean()

    # Trend (slope of last 14 days)
    recent = df.tail(14)
    if len(recent) >= 2:
        x = np.arange(len(recent))
        y = recent['revenue'].values
        slope, _ = np.polyfit(x, y, 1)
    else:
        slope = 0

    # Day-of-week factors
    dow_avg = df.groupby('day_of_week')['revenue'].mean()
    overall_avg = df['revenue'].mean()
    dow_factors = (dow_avg / overall_avg).to_dict() if overall_avg > 0 else {}

    # Generate forecast
    last_date = df['order_date'].max()
    last_ma = df['ma_7'].iloc[-1]

    forecast_dates = []
    forecast_revenue = []

    for i in range(1, forecast_days + 1):
        future_date = last_date + timedelta(days=i)
        dow = future_date.isoweekday()
        dow_bq = dow % 7 + 1  # BigQuery format (1=Sunday)

        # Base + damped trend
        base = last_ma + (slope * i * 0.3)

        # Apply day-of-week factor
        dow_factor = dow_factors.get(dow_bq, 1.0)
        projected = max(0, base * dow_factor)

        forecast_dates.append(future_date)
        forecast_revenue.append(projected)

    return pd.DataFrame({
        'order_date': forecast_dates,
        'forecast_revenue': forecast_revenue
    })


# ============================================================================
# FORMATTING HELPERS
# ============================================================================

def format_currency(val):
    if pd.isna(val) or val == 0:
        return "$0"
    if val >= 1_000_000:
        return f"${val/1_000_000:.1f}M"
    if val >= 1_000:
        return f"${val/1_000:.0f}K"
    return f"${val:,.0f}"


def format_number(val):
    if pd.isna(val):
        return "-"
    return f"{val:,.0f}"


def format_percent(val):
    if pd.isna(val):
        return "-"
    return f"{val:.1f}%"


def render_kpi(value, label, delta=None):
    """Render a KPI card."""
    delta_html = ""
    if delta is not None:
        delta_class = "kpi-delta-positive" if delta >= 0 else "kpi-delta-negative"
        delta_symbol = "+" if delta >= 0 else ""
        delta_html = f'<div class="{delta_class}">{delta_symbol}{delta:.1f}% WoW</div>'

    return f"""
    <div class="kpi-card">
        <div class="kpi-value">{value}</div>
        <div class="kpi-label">{label}</div>
        {delta_html}
    </div>
    """


def apply_dark_theme(fig, height=350):
    """Apply dark theme to plotly figure."""
    fig.update_layout(
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font=dict(color='#ccd6f6'),
        height=height,
        margin=dict(l=20, r=20, t=40, b=20),
        xaxis=dict(gridcolor='rgba(255,255,255,0.1)', linecolor='rgba(255,255,255,0.1)'),
        yaxis=dict(gridcolor='rgba(255,255,255,0.1)', linecolor='rgba(255,255,255,0.1)'),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, font=dict(color='#8892b0'))
    )
    return fig


# ============================================================================
# SIDEBAR
# ============================================================================

with st.sidebar:
    st.markdown("### Settings")
    lookback_days = st.selectbox(
        "Date Range",
        options=[30, 60, 90, 180],
        index=2,
        format_func=lambda x: f"Last {x} days"
    )

    forecast_days = st.selectbox(
        "Forecast Period",
        options=[7, 14, 30, 60],
        index=2,
        format_func=lambda x: f"{x} days"
    )

    if st.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()


# ============================================================================
# MAIN CONTENT
# ============================================================================

st.title("📊 Sales Dashboard")
st.caption("Combined B2B (Salesforce) + B2C (Shopify) Revenue")

# Load data
try:
    b2b_daily = load_b2b_daily(lookback_days)
    b2b_weekly = load_b2b_weekly(lookback_days)
    b2b_accounts = load_b2b_by_account(lookback_days)
    b2c_daily = load_b2c_daily(lookback_days)
    b2c_weekly = load_b2c_weekly(lookback_days)
    b2c_products = load_b2c_products(lookback_days)
    owner_options, account_type_options = load_filter_options()
except Exception as e:
    st.error(f"Error loading data: {e}")
    st.stop()

# Calculate totals
b2b_total = b2b_daily['revenue'].sum() if not b2b_daily.empty else 0
b2c_total = b2c_daily['revenue'].sum() if not b2c_daily.empty else 0
total_revenue = b2b_total + b2c_total
b2b_orders = b2b_daily['order_count'].sum() if not b2b_daily.empty else 0
b2c_orders = b2c_daily['order_count'].sum() if not b2c_daily.empty else 0
total_orders = b2b_orders + b2c_orders

# Channel mix
b2b_pct = (b2b_total / total_revenue * 100) if total_revenue > 0 else 0
b2c_pct = (b2c_total / total_revenue * 100) if total_revenue > 0 else 0

# Combined forecast
combined_daily = pd.DataFrame()
if not b2b_daily.empty and not b2c_daily.empty:
    b2b_temp = b2b_daily[['order_date', 'revenue', 'day_of_week']].copy()
    b2c_temp = b2c_daily[['order_date', 'revenue', 'day_of_week']].copy()
    combined_daily = pd.merge(b2b_temp, b2c_temp, on=['order_date', 'day_of_week'], how='outer', suffixes=('_b2b', '_b2c'))
    # Only fill numeric columns with 0, not datetime columns
    combined_daily['revenue_b2b'] = combined_daily['revenue_b2b'].fillna(0)
    combined_daily['revenue_b2c'] = combined_daily['revenue_b2c'].fillna(0)
    combined_daily['revenue'] = combined_daily['revenue_b2b'] + combined_daily['revenue_b2c']

forecast_df = calculate_seasonal_forecast(combined_daily, forecast_days) if not combined_daily.empty else pd.DataFrame()
forecast_total = forecast_df['forecast_revenue'].sum() if not forecast_df.empty else 0

# WoW change calculation
if len(b2b_weekly) >= 2:
    b2b_wow = ((b2b_weekly.iloc[-1]['revenue'] - b2b_weekly.iloc[-2]['revenue']) / b2b_weekly.iloc[-2]['revenue'] * 100) if b2b_weekly.iloc[-2]['revenue'] > 0 else 0
else:
    b2b_wow = None

if len(b2c_weekly) >= 2:
    b2c_wow = ((b2c_weekly.iloc[-1]['revenue'] - b2c_weekly.iloc[-2]['revenue']) / b2c_weekly.iloc[-2]['revenue'] * 100) if b2c_weekly.iloc[-2]['revenue'] > 0 else 0
else:
    b2c_wow = None


# ============================================================================
# TABS
# ============================================================================

tab1, tab2, tab3, tab4 = st.tabs([
    "📈 Overview",
    "🏢 B2B Performance",
    "🛒 B2C Performance",
    "🔮 Forecast"
])

# ============================================================================
# TAB 1: EXECUTIVE OVERVIEW
# ============================================================================
with tab1:
    # KPI Row
    col1, col2, col3, col4, col5, col6 = st.columns(6)

    with col1:
        st.markdown(render_kpi(format_currency(total_revenue), f"Total Revenue ({lookback_days}d)"), unsafe_allow_html=True)
    with col2:
        st.markdown(render_kpi(format_currency(b2b_total), f"B2B ({b2b_pct:.0f}%)", b2b_wow), unsafe_allow_html=True)
    with col3:
        st.markdown(render_kpi(format_currency(b2c_total), f"B2C ({b2c_pct:.0f}%)", b2c_wow), unsafe_allow_html=True)
    with col4:
        st.markdown(render_kpi(format_number(total_orders), "Total Orders"), unsafe_allow_html=True)
    with col5:
        aov = total_revenue / total_orders if total_orders > 0 else 0
        st.markdown(render_kpi(format_currency(aov), "Avg Order Value"), unsafe_allow_html=True)
    with col6:
        st.markdown(render_kpi(format_currency(forecast_total), f"{forecast_days}d Forecast"), unsafe_allow_html=True)

    st.divider()

    # Charts Row 1: Stacked Revenue Trend
    st.subheader("Daily Revenue by Channel")

    if not combined_daily.empty:
        fig = go.Figure()

        # B2B (bottom layer)
        fig.add_trace(go.Scatter(
            x=combined_daily['order_date'],
            y=combined_daily['revenue_b2b'],
            name='B2B (Salesforce)',
            fill='tozeroy',
            line=dict(color=COLORS['b2b'], width=0),
            fillcolor='rgba(46, 134, 171, 0.7)',
            stackgroup='revenue'
        ))

        # B2C (top layer)
        fig.add_trace(go.Scatter(
            x=combined_daily['order_date'],
            y=combined_daily['revenue_b2c'],
            name='B2C (Shopify)',
            fill='tonexty',
            line=dict(color=COLORS['b2c'], width=0),
            fillcolor='rgba(0, 212, 170, 0.7)',
            stackgroup='revenue'
        ))

        apply_dark_theme(fig, height=400)
        fig.update_layout(hovermode='x unified')
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No data available for the selected period")

    # Charts Row 2: Weekly Comparison + Channel Mix
    col1, col2 = st.columns([2, 1])

    with col1:
        st.subheader("Weekly Revenue by Channel")

        if not b2b_weekly.empty or not b2c_weekly.empty:
            # Merge weekly data
            weekly_combined = pd.merge(
                b2b_weekly[['week_start', 'revenue']].rename(columns={'revenue': 'B2B'}),
                b2c_weekly[['week_start', 'revenue']].rename(columns={'revenue': 'B2C'}),
                on='week_start', how='outer'
            ).fillna(0)

            fig = go.Figure()
            fig.add_trace(go.Bar(x=weekly_combined['week_start'], y=weekly_combined['B2B'], name='B2B', marker_color=COLORS['b2b']))
            fig.add_trace(go.Bar(x=weekly_combined['week_start'], y=weekly_combined['B2C'], name='B2C', marker_color=COLORS['b2c']))

            apply_dark_theme(fig, height=350)
            fig.update_layout(barmode='stack')
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Channel Mix")

        mix_data = pd.DataFrame({
            'Channel': ['B2B', 'B2C'],
            'Revenue': [b2b_total, b2c_total]
        })

        fig = px.pie(mix_data, values='Revenue', names='Channel',
                     color='Channel', color_discrete_map={'B2B': COLORS['b2b'], 'B2C': COLORS['b2c']},
                     hole=0.4)
        apply_dark_theme(fig, height=350)
        st.plotly_chart(fig, use_container_width=True)


# ============================================================================
# TAB 2: B2B PERFORMANCE
# ============================================================================
with tab2:
    st.subheader("B2B Performance (Salesforce)")

    # Filters
    col1, col2 = st.columns(2)
    with col1:
        selected_owners = st.multiselect("Filter by Account Owner", options=owner_options, default=[])
    with col2:
        selected_types = st.multiselect("Filter by Account Type", options=account_type_options, default=[])

    # Apply filters
    filtered_accounts = b2b_accounts.copy()
    if selected_owners:
        filtered_accounts = filtered_accounts[filtered_accounts['owner_name'].isin(selected_owners)]
    if selected_types:
        filtered_accounts = filtered_accounts[filtered_accounts['account_type'].isin(selected_types)]

    # B2B KPIs (filtered)
    filtered_revenue = filtered_accounts['revenue'].sum()
    filtered_orders = filtered_accounts['order_count'].sum()
    filtered_accounts_count = len(filtered_accounts)

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Revenue", format_currency(filtered_revenue))
    col2.metric("Orders", format_number(filtered_orders))
    col3.metric("Accounts", format_number(filtered_accounts_count))
    col4.metric("Avg Order Value", format_currency(filtered_revenue / filtered_orders if filtered_orders > 0 else 0))

    st.divider()

    # Top Accounts Table
    col1, col2 = st.columns([1, 1])

    with col1:
        st.subheader("Top 10 Accounts by Revenue")
        top_accounts = filtered_accounts.head(10)[['customer_name', 'account_type', 'owner_name', 'revenue', 'order_count']].copy()
        top_accounts['revenue'] = top_accounts['revenue'].apply(format_currency)
        top_accounts['order_count'] = top_accounts['order_count'].apply(format_number)
        top_accounts.columns = ['Account', 'Type', 'Owner', 'Revenue', 'Orders']
        st.dataframe(top_accounts, hide_index=True, use_container_width=True)

    with col2:
        st.subheader("Revenue by Account Type")
        type_summary = filtered_accounts.groupby('account_type')['revenue'].sum().reset_index()
        type_summary = type_summary.sort_values('revenue', ascending=True)

        fig = px.bar(type_summary, x='revenue', y='account_type', orientation='h',
                     color_discrete_sequence=[COLORS['b2b']])
        apply_dark_theme(fig, height=300)
        fig.update_layout(showlegend=False, xaxis_title='Revenue', yaxis_title='')
        st.plotly_chart(fig, use_container_width=True)

    # Weekly B2B Trend
    st.subheader("Weekly B2B Revenue")
    if not b2b_weekly.empty:
        fig = go.Figure()
        fig.add_trace(go.Bar(x=b2b_weekly['week_start'], y=b2b_weekly['revenue'],
                             marker_color=COLORS['b2b'], name='Revenue'))
        apply_dark_theme(fig, height=300)
        st.plotly_chart(fig, use_container_width=True)


# ============================================================================
# TAB 3: B2C PERFORMANCE
# ============================================================================
with tab3:
    st.subheader("B2C Performance (Shopify)")

    # B2C KPIs
    b2c_aov = b2c_total / b2c_orders if b2c_orders > 0 else 0

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Revenue", format_currency(b2c_total))
    col2.metric("Orders", format_number(b2c_orders))
    col3.metric("AOV", format_currency(b2c_aov))
    col4.metric("Daily Avg", format_currency(b2c_total / lookback_days if lookback_days > 0 else 0))

    st.divider()

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Day of Week Performance")
        if not b2c_daily.empty:
            dow_names = {1: 'Sun', 2: 'Mon', 3: 'Tue', 4: 'Wed', 5: 'Thu', 6: 'Fri', 7: 'Sat'}
            dow_data = b2c_daily.groupby('day_of_week')['revenue'].mean().reset_index()
            dow_data['day_name'] = dow_data['day_of_week'].map(dow_names)
            dow_data = dow_data.sort_values('day_of_week')

            fig = go.Figure()
            fig.add_trace(go.Bar(x=dow_data['day_name'], y=dow_data['revenue'], marker_color=COLORS['b2c']))
            apply_dark_theme(fig, height=300)
            fig.update_layout(xaxis_title='', yaxis_title='Avg Daily Revenue')
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Top Products by Revenue")
        if not b2c_products.empty:
            top_products = b2c_products.head(8)
            fig = px.bar(top_products, x='revenue', y='product_name', orientation='h',
                         color_discrete_sequence=[COLORS['b2c']])
            apply_dark_theme(fig, height=300)
            fig.update_layout(yaxis={'categoryorder': 'total ascending'}, showlegend=False,
                              xaxis_title='Revenue', yaxis_title='')
            st.plotly_chart(fig, use_container_width=True)

    # Product Table
    st.subheader("Product Performance")
    if not b2c_products.empty:
        display_products = b2c_products.head(15).copy()
        display_products['revenue'] = display_products['revenue'].apply(format_currency)
        display_products['avg_price'] = display_products['avg_price'].apply(lambda x: f"${x:.2f}")
        display_products = display_products[['product_name', 'units_sold', 'revenue', 'order_count', 'avg_price']]
        display_products.columns = ['Product', 'Units Sold', 'Revenue', 'Orders', 'Avg Price']
        st.dataframe(display_products, hide_index=True, use_container_width=True)


# ============================================================================
# TAB 4: FORECAST
# ============================================================================
with tab4:
    st.subheader("Revenue Forecast")

    # Forecast KPIs
    daily_forecast_avg = forecast_total / forecast_days if forecast_days > 0 else 0
    monthly_run_rate = daily_forecast_avg * 30

    col1, col2, col3 = st.columns(3)
    col1.metric(f"{forecast_days}-Day Forecast", format_currency(forecast_total))
    col2.metric("Daily Forecast Avg", format_currency(daily_forecast_avg))
    col3.metric("Monthly Run Rate", format_currency(monthly_run_rate))

    st.divider()

    # Forecast Chart
    st.subheader("Combined Revenue Forecast")

    if not combined_daily.empty and not forecast_df.empty:
        fig = go.Figure()

        # Historical (combined)
        fig.add_trace(go.Scatter(
            x=combined_daily['order_date'],
            y=combined_daily['revenue'],
            name='Actual',
            line=dict(color=COLORS['total'], width=2),
            fill='tozeroy',
            fillcolor='rgba(102, 126, 234, 0.2)'
        ))

        # 7-day MA
        combined_daily['ma_7'] = combined_daily['revenue'].rolling(window=7, min_periods=1).mean()
        fig.add_trace(go.Scatter(
            x=combined_daily['order_date'],
            y=combined_daily['ma_7'],
            name='7-Day Avg',
            line=dict(color='#8892b0', width=2, dash='solid')
        ))

        # Forecast
        fig.add_trace(go.Scatter(
            x=forecast_df['order_date'],
            y=forecast_df['forecast_revenue'],
            name='Forecast',
            line=dict(color=COLORS['forecast'], width=2, dash='dash'),
            fill='tozeroy',
            fillcolor='rgba(255, 214, 102, 0.2)'
        ))

        apply_dark_theme(fig, height=450)
        fig.update_layout(hovermode='x unified')
        st.plotly_chart(fig, use_container_width=True)

    # Channel Breakdown Forecast
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("B2B Forecast")
        b2b_forecast = calculate_seasonal_forecast(b2b_daily, forecast_days) if not b2b_daily.empty else pd.DataFrame()
        if not b2b_forecast.empty:
            b2b_forecast_total = b2b_forecast['forecast_revenue'].sum()
            st.metric(f"{forecast_days}-Day B2B Forecast", format_currency(b2b_forecast_total))

            fig = go.Figure()
            fig.add_trace(go.Scatter(x=b2b_daily['order_date'], y=b2b_daily['revenue'], name='Actual', line=dict(color=COLORS['b2b'])))
            fig.add_trace(go.Scatter(x=b2b_forecast['order_date'], y=b2b_forecast['forecast_revenue'], name='Forecast', line=dict(color=COLORS['forecast'], dash='dash')))
            apply_dark_theme(fig, height=250)
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("B2C Forecast")
        b2c_forecast = calculate_seasonal_forecast(b2c_daily, forecast_days) if not b2c_daily.empty else pd.DataFrame()
        if not b2c_forecast.empty:
            b2c_forecast_total = b2c_forecast['forecast_revenue'].sum()
            st.metric(f"{forecast_days}-Day B2C Forecast", format_currency(b2c_forecast_total))

            fig = go.Figure()
            fig.add_trace(go.Scatter(x=b2c_daily['order_date'], y=b2c_daily['revenue'], name='Actual', line=dict(color=COLORS['b2c'])))
            fig.add_trace(go.Scatter(x=b2c_forecast['order_date'], y=b2c_forecast['forecast_revenue'], name='Forecast', line=dict(color=COLORS['forecast'], dash='dash')))
            apply_dark_theme(fig, height=250)
            st.plotly_chart(fig, use_container_width=True)

    # Methodology Note
    st.divider()
    st.markdown("""
    **Forecast Methodology:**
    - Uses 7-day moving average as base
    - Applies day-of-week seasonality factors (weekends vs weekdays)
    - Includes trend adjustment (damped linear extrapolation)
    - B2B and B2C forecasted separately, then combined
    """)


# Footer
st.divider()
st.caption(f"Last refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M')} | Data: Salesforce + Shopify")
