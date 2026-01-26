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
def load_b2b_daily(start_date: str, end_date: str):
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
        AND sfo.order_date >= '{start_date}'
        AND sfo.order_date <= '{end_date}'
    GROUP BY sfo.order_date, day_of_week
    ORDER BY sfo.order_date
    """)


@st.cache_data(ttl=300)
def load_b2b_by_account(start_date: str, end_date: str):
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
        AND sfo.order_date >= '{start_date}'
        AND sfo.order_date <= '{end_date}'
    GROUP BY sfo.account_id, sfo.customer_name, sfo.account_type, u.Name
    ORDER BY revenue DESC
    """)


@st.cache_data(ttl=300)
def load_b2b_weekly(start_date: str, end_date: str):
    """Load B2B weekly aggregation."""
    return run_query(f"""
    SELECT
        DATE_TRUNC(order_date, WEEK(MONDAY)) as week_start,
        COUNT(DISTINCT order_id) as order_count,
        ROUND(SUM(CAST(line_total_price AS FLOAT64)), 2) as revenue,
        SUM(CAST(quantity AS FLOAT64)) as units
    FROM `artful-logic-475116-p1.staging_salesforce.salesforce_orders_flattened`
    WHERE order_status != 'Draft'
        AND order_date >= '{start_date}'
        AND order_date <= '{end_date}'
    GROUP BY week_start
    ORDER BY week_start
    """)


@st.cache_data(ttl=300)
def load_b2c_daily(start_date: str, end_date: str):
    """Load B2C (Shopify) daily gross sales."""
    return run_query(f"""
    SELECT
        DATE(created_at) as order_date,
        COUNT(DISTINCT id) as order_count,
        ROUND(SUM(CAST(total_line_items_price AS FLOAT64)), 2) as revenue,
        EXTRACT(DAYOFWEEK FROM created_at) as day_of_week
    FROM `artful-logic-475116-p1.raw_shopify.orders`
    WHERE cancelled_at IS NULL
        AND financial_status IN ('paid', 'partially_refunded')
        AND DATE(created_at) >= '{start_date}'
        AND DATE(created_at) <= '{end_date}'
    GROUP BY order_date, day_of_week
    ORDER BY order_date
    """)


@st.cache_data(ttl=300)
def load_b2c_weekly(start_date: str, end_date: str):
    """Load B2C weekly gross sales aggregation."""
    return run_query(f"""
    SELECT
        DATE_TRUNC(DATE(created_at), WEEK(MONDAY)) as week_start,
        COUNT(DISTINCT id) as order_count,
        ROUND(SUM(CAST(total_line_items_price AS FLOAT64)), 2) as revenue
    FROM `artful-logic-475116-p1.raw_shopify.orders`
    WHERE cancelled_at IS NULL
        AND financial_status IN ('paid', 'partially_refunded')
        AND DATE(created_at) >= '{start_date}'
        AND DATE(created_at) <= '{end_date}'
    GROUP BY week_start
    ORDER BY week_start
    """)


@st.cache_data(ttl=300)
def load_b2c_products(start_date: str, end_date: str):
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
            AND DATE(o.created_at) >= '{start_date}'
            AND DATE(o.created_at) <= '{end_date}'
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
# SKU DATA LOADING FUNCTIONS
# ============================================================================

@st.cache_data(ttl=300)
def load_b2b_sku_daily(start_date: str, end_date: str):
    """Load B2B (Salesforce) daily sales by SKU with product hierarchy."""
    return run_query(f"""
    SELECT
        sfo.order_date,
        sfo.product_code as sku,
        sfo.product_name,
        SUM(CAST(sfo.quantity AS FLOAT64)) as units,
        ROUND(SUM(CAST(sfo.line_total_price AS FLOAT64)), 2) as revenue,
        COUNT(DISTINCT sfo.order_id) as order_count
    FROM `artful-logic-475116-p1.staging_salesforce.salesforce_orders_flattened` sfo
    WHERE sfo.order_status != 'Draft'
        AND sfo.order_date >= '{start_date}'
        AND sfo.order_date <= '{end_date}'
        AND sfo.product_name IS NOT NULL
    GROUP BY sfo.order_date, sfo.product_code, sfo.product_name
    ORDER BY sfo.order_date, revenue DESC
    """)


@st.cache_data(ttl=300)
def load_b2b_sku_weekly(start_date: str, end_date: str):
    """Load B2B weekly aggregation by SKU."""
    return run_query(f"""
    SELECT
        DATE_TRUNC(order_date, WEEK(MONDAY)) as week_start,
        product_code as sku,
        product_name,
        SUM(CAST(quantity AS FLOAT64)) as units,
        ROUND(SUM(CAST(line_total_price AS FLOAT64)), 2) as revenue,
        COUNT(DISTINCT order_id) as order_count
    FROM `artful-logic-475116-p1.staging_salesforce.salesforce_orders_flattened`
    WHERE order_status != 'Draft'
        AND order_date >= '{start_date}'
        AND order_date <= '{end_date}'
        AND product_name IS NOT NULL
    GROUP BY week_start, product_code, product_name
    ORDER BY week_start, revenue DESC
    """)


@st.cache_data(ttl=300)
def load_b2c_sku_daily(start_date: str, end_date: str):
    """Load B2C (Shopify) daily sales by SKU."""
    return run_query(f"""
    WITH order_items AS (
        SELECT
            DATE(o.created_at) as order_date,
            JSON_VALUE(item, '$.title') as product_name,
            JSON_VALUE(item, '$.sku') as sku,
            CAST(JSON_VALUE(item, '$.quantity') AS INT64) as quantity,
            CAST(JSON_VALUE(item, '$.price') AS FLOAT64) as unit_price,
            o.id as order_id
        FROM `artful-logic-475116-p1.raw_shopify.orders` o,
        UNNEST(JSON_QUERY_ARRAY(o.line_items)) as item
        WHERE o.cancelled_at IS NULL
            AND o.financial_status IN ('paid', 'partially_refunded')
            AND DATE(o.created_at) >= '{start_date}'
            AND DATE(o.created_at) <= '{end_date}'
    )
    SELECT
        order_date,
        sku,
        product_name,
        SUM(quantity) as units,
        ROUND(SUM(quantity * unit_price), 2) as revenue,
        COUNT(DISTINCT order_id) as order_count
    FROM order_items
    WHERE product_name IS NOT NULL
        AND product_name NOT LIKE '%Shipping Protection%'
        AND product_name NOT LIKE '%Protectly%'
    GROUP BY order_date, sku, product_name
    HAVING units > 0
    ORDER BY order_date, revenue DESC
    """)


@st.cache_data(ttl=300)
def load_b2c_sku_weekly(start_date: str, end_date: str):
    """Load B2C weekly aggregation by SKU."""
    return run_query(f"""
    WITH order_items AS (
        SELECT
            DATE_TRUNC(DATE(o.created_at), WEEK(MONDAY)) as week_start,
            JSON_VALUE(item, '$.title') as product_name,
            JSON_VALUE(item, '$.sku') as sku,
            CAST(JSON_VALUE(item, '$.quantity') AS INT64) as quantity,
            CAST(JSON_VALUE(item, '$.price') AS FLOAT64) as unit_price,
            o.id as order_id
        FROM `artful-logic-475116-p1.raw_shopify.orders` o,
        UNNEST(JSON_QUERY_ARRAY(o.line_items)) as item
        WHERE o.cancelled_at IS NULL
            AND o.financial_status IN ('paid', 'partially_refunded')
            AND DATE(o.created_at) >= '{start_date}'
            AND DATE(o.created_at) <= '{end_date}'
    )
    SELECT
        week_start,
        sku,
        product_name,
        SUM(quantity) as units,
        ROUND(SUM(quantity * unit_price), 2) as revenue,
        COUNT(DISTINCT order_id) as order_count
    FROM order_items
    WHERE product_name IS NOT NULL
        AND product_name NOT LIKE '%Shipping Protection%'
        AND product_name NOT LIKE '%Protectly%'
    GROUP BY week_start, sku, product_name
    HAVING units > 0
    ORDER BY week_start, revenue DESC
    """)


def parse_product_hierarchy(product_name: str):
    """Parse product name into family, potency, and flavor."""
    if pd.isna(product_name):
        return 'Other', 'Unknown', 'Unknown'

    name_lower = product_name.lower()

    # Parse Family (format)
    if any(x in name_lower for x in ['shot', '2oz', '2 oz']):
        family = 'Shots'
    elif any(x in name_lower for x in ['750ml', '750 ml', 'bottle']):
        family = 'Bottles'
    elif any(x in name_lower for x in ['12oz', '12 oz']):
        family = '12oz Seltzers'
    elif any(x in name_lower for x in ['16oz', '16 oz']):
        family = '16oz Seltzers'
    elif 'seltzer' in name_lower:
        family = 'Seltzers'
    else:
        family = 'Other'

    # Parse Potency
    if '25mg' in name_lower:
        potency = '25mg'
    elif '10mg' in name_lower:
        potency = '10mg'
    elif '5mg' in name_lower:
        potency = '5mg'
    elif '2mg' in name_lower:
        potency = '2mg'
    else:
        potency = 'Unknown'

    # Parse Flavor
    flavor_map = {
        'berry': 'Berry',
        'citrus': 'Citrus',
        'tropical': 'Tropical',
        'spicy lime': 'Spicy Lime',
        'spicylime': 'Spicy Lime',
        'lemonade': 'Lemonade',
        'cherry': 'Cherry',
        'cranberry': 'Cranberry',
        'crnbry': 'Cranberry',
        'variety': 'Variety Pack',
        'original': 'Original',
    }

    flavor = 'Unknown'
    for key, val in flavor_map.items():
        if key in name_lower:
            flavor = val
            break

    return family, potency, flavor


def add_product_hierarchy(df: pd.DataFrame):
    """Add family, potency, flavor columns to dataframe."""
    if df.empty or 'product_name' not in df.columns:
        return df

    hierarchy = df['product_name'].apply(parse_product_hierarchy)
    df['family'] = hierarchy.apply(lambda x: x[0])
    df['potency'] = hierarchy.apply(lambda x: x[1])
    df['flavor'] = hierarchy.apply(lambda x: x[2])
    return df


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
        delta_html = f'<div class="{delta_class}">{delta_symbol}{delta:.1f}% MoM</div>'

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

# ============================================================================
# MAIN CONTENT
# ============================================================================

st.title("📊 Sales Dashboard")
st.caption("Combined B2B (Salesforce) + B2C (Shopify) Revenue")

# Date range selector at top of page
today = datetime.now().date()
ytd_start = datetime(today.year, 1, 1).date()

# Preset options
date_presets = {
    "YTD": (ytd_start, today),
    "Last 30 Days": (today - timedelta(days=30), today),
    "Last 60 Days": (today - timedelta(days=60), today),
    "Last 90 Days": (today - timedelta(days=90), today),
    "Last 180 Days": (today - timedelta(days=180), today),
    "Last 365 Days": (today - timedelta(days=365), today),
    "Custom": None
}

col1, col2, col3, col4 = st.columns([2, 2, 2, 1])

with col1:
    selected_preset = st.selectbox(
        "Date Range",
        options=list(date_presets.keys()),
        index=0  # Default to YTD
    )

if selected_preset == "Custom":
    with col2:
        start_date = st.date_input("Start Date", value=ytd_start)
    with col3:
        end_date = st.date_input("End Date", value=today)
else:
    start_date, end_date = date_presets[selected_preset]

with col4:
    forecast_days = st.selectbox("Forecast", options=[7, 14, 30, 60], index=2, format_func=lambda x: f"{x}d")

# Sidebar for refresh only
with st.sidebar:
    st.markdown("### Settings")
    if st.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()

st.divider()

# Convert dates to strings for queries
start_date_str = start_date.strftime('%Y-%m-%d')
end_date_str = end_date.strftime('%Y-%m-%d')
num_days = (end_date - start_date).days + 1

# Load data
try:
    b2b_daily = load_b2b_daily(start_date_str, end_date_str)
    b2b_weekly = load_b2b_weekly(start_date_str, end_date_str)
    b2b_accounts = load_b2b_by_account(start_date_str, end_date_str)
    b2c_daily = load_b2c_daily(start_date_str, end_date_str)
    b2c_weekly = load_b2c_weekly(start_date_str, end_date_str)
    b2c_products = load_b2c_products(start_date_str, end_date_str)
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

# MoM change calculation (day-of-month sensitive)
# Compare current month (1st to today) vs same period last month
current_day = today.day
current_month_start = today.replace(day=1)

# Last month same period
if today.month == 1:
    last_month_start = today.replace(year=today.year - 1, month=12, day=1)
    last_month_end = today.replace(year=today.year - 1, month=12, day=min(current_day, 31))
else:
    last_month_start = today.replace(month=today.month - 1, day=1)
    # Handle months with fewer days
    import calendar
    last_month_days = calendar.monthrange(today.year, today.month - 1)[1]
    last_month_end = today.replace(month=today.month - 1, day=min(current_day, last_month_days))

# Calculate MoM for B2B
if not b2b_daily.empty:
    b2b_daily['order_date'] = pd.to_datetime(b2b_daily['order_date'])
    current_month_b2b = b2b_daily[
        (b2b_daily['order_date'] >= pd.Timestamp(current_month_start)) &
        (b2b_daily['order_date'] <= pd.Timestamp(today))
    ]['revenue'].sum()
    last_month_b2b = b2b_daily[
        (b2b_daily['order_date'] >= pd.Timestamp(last_month_start)) &
        (b2b_daily['order_date'] <= pd.Timestamp(last_month_end))
    ]['revenue'].sum()
    b2b_mom = ((current_month_b2b - last_month_b2b) / last_month_b2b * 100) if last_month_b2b > 0 else None
else:
    b2b_mom = None

# Calculate MoM for B2C
if not b2c_daily.empty:
    b2c_daily['order_date'] = pd.to_datetime(b2c_daily['order_date'])
    current_month_b2c = b2c_daily[
        (b2c_daily['order_date'] >= pd.Timestamp(current_month_start)) &
        (b2c_daily['order_date'] <= pd.Timestamp(today))
    ]['revenue'].sum()
    last_month_b2c = b2c_daily[
        (b2c_daily['order_date'] >= pd.Timestamp(last_month_start)) &
        (b2c_daily['order_date'] <= pd.Timestamp(last_month_end))
    ]['revenue'].sum()
    b2c_mom = ((current_month_b2c - last_month_b2c) / last_month_b2c * 100) if last_month_b2c > 0 else None
else:
    b2c_mom = None


# ============================================================================
# TABS
# ============================================================================

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📈 Overview",
    "🏢 B2B Performance",
    "🛒 B2C Performance",
    "🔮 Forecast",
    "📦 SKU Performance"
])

# ============================================================================
# TAB 1: EXECUTIVE OVERVIEW
# ============================================================================
with tab1:
    # KPI Row
    col1, col2, col3, col4, col5, col6 = st.columns(6)

    with col1:
        st.markdown(render_kpi(format_currency(total_revenue), f"Total Revenue ({selected_preset})"), unsafe_allow_html=True)
    with col2:
        st.markdown(render_kpi(format_currency(b2b_total), f"B2B ({b2b_pct:.0f}%)", b2b_mom), unsafe_allow_html=True)
    with col3:
        st.markdown(render_kpi(format_currency(b2c_total), f"B2C ({b2c_pct:.0f}%)", b2c_mom), unsafe_allow_html=True)
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
            )
            weekly_combined['B2B'] = weekly_combined['B2B'].fillna(0)
            weekly_combined['B2C'] = weekly_combined['B2C'].fillna(0)

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
    col4.metric("Daily Avg", format_currency(b2c_total / num_days if num_days > 0 else 0))

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


# ============================================================================
# TAB 5: SKU PERFORMANCE
# ============================================================================
with tab5:
    st.subheader("SKU Performance Analysis")
    st.caption("Drill down from Family → Potency → Flavor → Individual SKUs")

    # Channel selector
    col1, col2, col3 = st.columns([1, 1, 2])
    with col1:
        sku_channel = st.radio("Channel", ["B2B", "B2C", "Combined"], horizontal=True, key="sku_channel")
    with col2:
        time_granularity = st.radio("View", ["Daily", "Weekly"], horizontal=True, key="sku_granularity")

    st.divider()

    # Load SKU data based on channel selection
    try:
        if sku_channel == "B2B":
            if time_granularity == "Daily":
                sku_data = load_b2b_sku_daily(start_date_str, end_date_str)
            else:
                sku_data = load_b2b_sku_weekly(start_date_str, end_date_str)
                if not sku_data.empty:
                    sku_data = sku_data.rename(columns={'week_start': 'order_date'})
        elif sku_channel == "B2C":
            if time_granularity == "Daily":
                sku_data = load_b2c_sku_daily(start_date_str, end_date_str)
            else:
                sku_data = load_b2c_sku_weekly(start_date_str, end_date_str)
                if not sku_data.empty:
                    sku_data = sku_data.rename(columns={'week_start': 'order_date'})
        else:  # Combined
            if time_granularity == "Daily":
                b2b_sku = load_b2b_sku_daily(start_date_str, end_date_str)
                b2c_sku = load_b2c_sku_daily(start_date_str, end_date_str)
            else:
                b2b_sku = load_b2b_sku_weekly(start_date_str, end_date_str)
                b2c_sku = load_b2c_sku_weekly(start_date_str, end_date_str)
                if not b2b_sku.empty:
                    b2b_sku = b2b_sku.rename(columns={'week_start': 'order_date'})
                if not b2c_sku.empty:
                    b2c_sku = b2c_sku.rename(columns={'week_start': 'order_date'})

            if not b2b_sku.empty:
                b2b_sku['channel'] = 'B2B'
            if not b2c_sku.empty:
                b2c_sku['channel'] = 'B2C'
            sku_data = pd.concat([b2b_sku, b2c_sku], ignore_index=True) if not b2b_sku.empty or not b2c_sku.empty else pd.DataFrame()

        # Add product hierarchy
        if not sku_data.empty:
            sku_data = add_product_hierarchy(sku_data)
    except Exception as e:
        st.error(f"Error loading SKU data: {e}")
        sku_data = pd.DataFrame()

    if sku_data.empty:
        st.info("No SKU data available for the selected period")
    else:
        # Summary KPIs
        total_sku_revenue = sku_data['revenue'].sum()
        total_sku_units = sku_data['units'].sum()
        unique_skus = sku_data['sku'].nunique()
        unique_families = sku_data['family'].nunique()

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Revenue", format_currency(total_sku_revenue))
        col2.metric("Total Units", format_number(total_sku_units))
        col3.metric("Unique SKUs", format_number(unique_skus))
        col4.metric("Product Families", format_number(unique_families))

        st.divider()

        # Hierarchical Drilldown Section
        st.markdown("### Drilldown Filters")

        # Level 1: Family
        family_summary = sku_data.groupby('family').agg({
            'revenue': 'sum',
            'units': 'sum',
            'sku': 'nunique'
        }).reset_index().sort_values('revenue', ascending=False)

        col1, col2, col3 = st.columns(3)

        with col1:
            family_options = ['All Families'] + family_summary['family'].tolist()
            selected_family = st.selectbox("Family (Format)", family_options, key="family_filter")

        # Filter by family
        if selected_family != 'All Families':
            filtered_data = sku_data[sku_data['family'] == selected_family]
        else:
            filtered_data = sku_data

        # Level 2: Potency (based on family filter)
        with col2:
            potency_options = ['All Potencies'] + sorted(filtered_data['potency'].unique().tolist())
            selected_potency = st.selectbox("Potency", potency_options, key="potency_filter")

        # Filter by potency
        if selected_potency != 'All Potencies':
            filtered_data = filtered_data[filtered_data['potency'] == selected_potency]

        # Level 3: Flavor (based on family + potency filter)
        with col3:
            flavor_options = ['All Flavors'] + sorted(filtered_data['flavor'].unique().tolist())
            selected_flavor = st.selectbox("Flavor", flavor_options, key="flavor_filter")

        # Filter by flavor
        if selected_flavor != 'All Flavors':
            filtered_data = filtered_data[filtered_data['flavor'] == selected_flavor]

        st.divider()

        # Charts Row 1: Time Series by current drill level
        col1, col2 = st.columns([2, 1])

        with col1:
            current_level = "SKU"
            if selected_flavor == 'All Flavors' and selected_potency == 'All Potencies' and selected_family == 'All Families':
                current_level = "Family"
                group_col = 'family'
            elif selected_flavor == 'All Flavors' and selected_potency == 'All Potencies':
                current_level = "Potency"
                group_col = 'potency'
            elif selected_flavor == 'All Flavors':
                current_level = "Flavor"
                group_col = 'flavor'
            else:
                current_level = "SKU"
                group_col = 'product_name'

            st.subheader(f"Revenue Over Time by {current_level}")

            # Aggregate by time and group
            time_series = filtered_data.groupby(['order_date', group_col]).agg({
                'revenue': 'sum'
            }).reset_index()

            if not time_series.empty:
                # Get top groups by total revenue
                top_groups = time_series.groupby(group_col)['revenue'].sum().nlargest(8).index.tolist()
                time_series_top = time_series[time_series[group_col].isin(top_groups)]

                fig = px.line(
                    time_series_top,
                    x='order_date',
                    y='revenue',
                    color=group_col,
                    title=None,
                    markers=time_granularity == "Weekly"
                )
                apply_dark_theme(fig, height=400)
                fig.update_layout(
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                    hovermode='x unified'
                )
                st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.subheader(f"Revenue by {current_level}")

            # Summary by group
            group_summary = filtered_data.groupby(group_col).agg({
                'revenue': 'sum',
                'units': 'sum'
            }).reset_index().sort_values('revenue', ascending=True).tail(10)

            fig = px.bar(
                group_summary,
                x='revenue',
                y=group_col,
                orientation='h',
                color_discrete_sequence=[COLORS['b2b'] if sku_channel == 'B2B' else COLORS['b2c']]
            )
            apply_dark_theme(fig, height=400)
            fig.update_layout(showlegend=False, xaxis_title='Revenue', yaxis_title='')
            st.plotly_chart(fig, use_container_width=True)

        # Charts Row 2: Units trend + Mix breakdown
        col1, col2 = st.columns(2)

        with col1:
            st.subheader(f"Units Sold Over Time")

            time_series_units = filtered_data.groupby(['order_date', group_col]).agg({
                'units': 'sum'
            }).reset_index()

            if not time_series_units.empty:
                top_groups_units = time_series_units.groupby(group_col)['units'].sum().nlargest(8).index.tolist()
                time_series_units_top = time_series_units[time_series_units[group_col].isin(top_groups_units)]

                fig = go.Figure()
                for grp in top_groups_units:
                    grp_data = time_series_units_top[time_series_units_top[group_col] == grp]
                    fig.add_trace(go.Scatter(
                        x=grp_data['order_date'],
                        y=grp_data['units'],
                        name=grp[:25],
                        mode='lines+markers' if time_granularity == "Weekly" else 'lines',
                        stackgroup='one'
                    ))
                apply_dark_theme(fig, height=350)
                fig.update_layout(
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font=dict(size=10)),
                    hovermode='x unified'
                )
                st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.subheader("Revenue Mix")

            mix_data = filtered_data.groupby(group_col)['revenue'].sum().reset_index()
            mix_data = mix_data.sort_values('revenue', ascending=False).head(8)

            fig = px.pie(
                mix_data,
                values='revenue',
                names=group_col,
                hole=0.4
            )
            apply_dark_theme(fig, height=350)
            fig.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig, use_container_width=True)

        # =====================================================================
        # INDIVIDUAL SKU PERFORMANCE SECTION
        # =====================================================================
        st.divider()
        st.markdown("### Individual SKU Performance")

        # Get list of products ranked by revenue (use product_name as primary key)
        sku_rankings = filtered_data.groupby('product_name').agg({
            'revenue': 'sum',
            'units': 'sum'
        }).reset_index().sort_values('revenue', ascending=False)

        # SKU multi-select (select up to 5)
        col1, col2 = st.columns([3, 1])
        with col1:
            sku_options = sku_rankings['product_name'].tolist()
            # Default to top 3 SKUs if available
            default_skus = sku_options[:3] if len(sku_options) >= 3 else sku_options
            selected_skus = st.multiselect(
                "Select Products to Compare (up to 5)",
                options=sku_options,
                default=default_skus,
                max_selections=5,
                key="sku_selector",
                format_func=lambda x: f"{x[:50]}..." if len(x) > 50 else x
            )
        with col2:
            sku_metric = st.radio("Metric", ["Revenue", "Units"], horizontal=True, key="sku_metric")

        if selected_skus:
            # Filter to selected SKUs
            sku_time_data = filtered_data[filtered_data['product_name'].isin(selected_skus)]

            # Individual SKU Time Series
            st.subheader(f"SKU {sku_metric} Over Time")

            metric_col = 'revenue' if sku_metric == "Revenue" else 'units'

            # Aggregate by date and SKU
            sku_ts = sku_time_data.groupby(['order_date', 'product_name']).agg({
                metric_col: 'sum'
            }).reset_index()

            if not sku_ts.empty:
                fig = go.Figure()

                for sku_name in selected_skus:
                    sku_subset = sku_ts[sku_ts['product_name'] == sku_name]
                    # Truncate long names for legend
                    legend_name = sku_name[:35] + "..." if len(sku_name) > 35 else sku_name
                    fig.add_trace(go.Scatter(
                        x=sku_subset['order_date'],
                        y=sku_subset[metric_col],
                        name=legend_name,
                        mode='lines+markers' if time_granularity == "Weekly" else 'lines',
                        hovertemplate=f"<b>{sku_name[:30]}</b><br>" +
                                      "%{x}<br>" +
                                      f"{sku_metric}: %{{y:,.0f}}<extra></extra>"
                    ))

                apply_dark_theme(fig, height=400)
                fig.update_layout(
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0, font=dict(size=10)),
                    hovermode='x unified',
                    yaxis_title=sku_metric
                )
                st.plotly_chart(fig, use_container_width=True)

            # SKU Comparison Cards
            st.subheader("SKU Comparison")

            # Calculate metrics for each selected SKU
            sku_metrics = []
            for sku_name in selected_skus:
                sku_subset = sku_time_data[sku_time_data['product_name'] == sku_name]
                total_rev = sku_subset['revenue'].sum()
                total_units = sku_subset['units'].sum()
                num_days = sku_subset['order_date'].nunique()
                avg_daily_rev = total_rev / num_days if num_days > 0 else 0
                avg_daily_units = total_units / num_days if num_days > 0 else 0

                # Get hierarchy info
                sku_info = filtered_data[filtered_data['product_name'] == sku_name].iloc[0] if not filtered_data[filtered_data['product_name'] == sku_name].empty else None

                sku_metrics.append({
                    'name': sku_name,
                    'revenue': total_rev,
                    'units': total_units,
                    'avg_daily_rev': avg_daily_rev,
                    'avg_daily_units': avg_daily_units,
                    'days_with_sales': num_days,
                    'family': sku_info['family'] if sku_info is not None else 'Unknown',
                    'potency': sku_info['potency'] if sku_info is not None else 'Unknown',
                    'flavor': sku_info['flavor'] if sku_info is not None else 'Unknown',
                })

            # Display as columns (up to 5)
            cols = st.columns(len(selected_skus))
            for i, (col, metrics) in enumerate(zip(cols, sku_metrics)):
                with col:
                    st.markdown(f"**{metrics['name'][:30]}{'...' if len(metrics['name']) > 30 else ''}**")
                    st.caption(f"{metrics['family']} · {metrics['potency']} · {metrics['flavor']}")
                    st.metric("Revenue", format_currency(metrics['revenue']))
                    st.metric("Units", format_number(metrics['units']))
                    st.metric("Avg Daily Rev", format_currency(metrics['avg_daily_rev']))
                    st.metric("Days w/ Sales", format_number(metrics['days_with_sales']))

        else:
            st.info("Select SKUs above to see individual performance trends")

        # SKU Rankings Table
        st.divider()
        st.subheader("All SKUs Ranked by Revenue")

        # Aggregate to SKU level with full details
        sku_table = filtered_data.groupby(['sku', 'product_name', 'family', 'potency', 'flavor']).agg({
            'revenue': 'sum',
            'units': 'sum',
            'order_count': 'sum'
        }).reset_index().sort_values('revenue', ascending=False)

        if not sku_table.empty:
            # Add rank column
            sku_table['rank'] = range(1, len(sku_table) + 1)

            display_sku = sku_table.head(50).copy()
            display_sku['revenue_fmt'] = display_sku['revenue'].apply(format_currency)
            display_sku['units_fmt'] = display_sku['units'].apply(format_number)
            display_sku = display_sku[['rank', 'product_name', 'family', 'potency', 'flavor', 'revenue_fmt', 'units_fmt', 'order_count']]
            display_sku.columns = ['#', 'Product Name', 'Family', 'Potency', 'Flavor', 'Revenue', 'Units', 'Orders']
            st.dataframe(display_sku, hide_index=True, use_container_width=True, height=400)


# Footer
st.divider()
st.caption(f"Last refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M')} | Data: Salesforce + Shopify")
