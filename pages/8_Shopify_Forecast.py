"""
Shopify B2C Sales Forecasting Dashboard
Analyzes Shopify order data to forecast B2C revenue, track product performance,
and identify sales trends.
"""

import streamlit as st
import pandas as pd
from google.cloud import bigquery
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go
import numpy as np

st.set_page_config(page_title="Shopify Forecast", page_icon="🛒", layout="wide")

# Dark mode CSS
st.markdown("""
<style>
    .stApp { background: linear-gradient(135deg, #0f0f1a 0%, #1a1a2e 50%, #16213e 100%); }
    #MainMenu, footer, header { visibility: hidden; }
    .metric-card {
        background: linear-gradient(145deg, #1e1e2f 0%, #2a2a4a 100%);
        border-radius: 16px; padding: 24px;
        border: 1px solid rgba(255,255,255,0.1);
        box-shadow: 0 8px 32px rgba(0,0,0,0.3);
    }
    .metric-value {
        font-size: 42px; font-weight: 700;
        background: linear-gradient(135deg, #00d4aa 0%, #00a3cc 100%);
        -webkit-background-clip: text; -webkit-text-fill-color: transparent;
    }
    .metric-value-warning {
        font-size: 42px; font-weight: 700;
        background: linear-gradient(135deg, #ffd666 0%, #ff9f43 100%);
        -webkit-background-clip: text; -webkit-text-fill-color: transparent;
    }
    .metric-value-success {
        font-size: 42px; font-weight: 700;
        background: linear-gradient(135deg, #64ffda 0%, #00d4aa 100%);
        -webkit-background-clip: text; -webkit-text-fill-color: transparent;
    }
    .metric-value-info {
        font-size: 42px; font-weight: 700;
        background: linear-gradient(135deg, #74b9ff 0%, #667eea 100%);
        -webkit-background-clip: text; -webkit-text-fill-color: transparent;
    }
    .metric-label { font-size: 14px; color: #8892b0; text-transform: uppercase; letter-spacing: 1.5px; margin-top: 8px; }
    .dashboard-header {
        background: linear-gradient(90deg, #00d4aa 0%, #00a3cc 100%);
        -webkit-background-clip: text; -webkit-text-fill-color: transparent;
        font-size: 48px; font-weight: 800;
    }
    .dashboard-subtitle { color: #8892b0; font-size: 16px; margin-bottom: 32px; }
    .section-header {
        color: #ccd6f6; font-size: 24px; font-weight: 600;
        margin: 32px 0 16px 0; padding-bottom: 8px;
        border-bottom: 2px solid rgba(0, 212, 170, 0.3);
    }
    .filter-container {
        background: linear-gradient(145deg, #1e1e2f 0%, #2a2a4a 100%);
        border-radius: 16px; padding: 20px; margin-bottom: 24px;
        border: 1px solid rgba(255,255,255,0.1);
    }
    .forecast-banner {
        background: linear-gradient(145deg, #1e3a5f 0%, #2a4a6a 100%);
        border-radius: 12px; padding: 16px 24px;
        border-left: 4px solid #00d4aa; margin: 16px 0;
    }
    .live-indicator { display: inline-flex; align-items: center; gap: 8px; color: #64ffda; font-size: 12px; text-transform: uppercase; }
    .live-dot { width: 8px; height: 8px; background: #64ffda; border-radius: 50%; animation: pulse 2s infinite; }
    @keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.5; } }
</style>
""", unsafe_allow_html=True)

COLORS = {
    'primary': '#00d4aa', 'secondary': '#00a3cc', 'success': '#64ffda',
    'warning': '#ffd666', 'danger': '#ff6b6b', 'info': '#74b9ff', 'purple': '#667eea'
}

def apply_dark_theme(fig, height=350, **kwargs):
    xaxis_defaults = {'gridcolor': 'rgba(255,255,255,0.1)', 'tickfont': {'color': '#8892b0'}}
    yaxis_defaults = {'gridcolor': 'rgba(255,255,255,0.1)', 'tickfont': {'color': '#8892b0'}}
    xaxis_defaults.update(kwargs.pop('xaxis', {}))
    yaxis_defaults.update(kwargs.pop('yaxis', {}))
    margin = kwargs.pop('margin', dict(l=0, r=0, t=20, b=0))
    fig.update_layout(
        paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
        font={'color': '#ccd6f6'}, height=height, margin=margin,
        xaxis=xaxis_defaults, yaxis=yaxis_defaults, **kwargs
    )
    return fig

@st.cache_resource
def get_bq_client():
    try:
        if "gcp_service_account" in st.secrets:
            from google.oauth2 import service_account
            credentials = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])
            return bigquery.Client(project='artful-logic-475116-p1', credentials=credentials)
    except Exception:
        pass
    return bigquery.Client(project='artful-logic-475116-p1')

@st.cache_data(ttl=300)
def load_daily_sales(lookback_days: int = 90):
    client = get_bq_client()
    query = f"""
    SELECT
        DATE(created_at) as order_date,
        COUNT(DISTINCT id) as order_count,
        SUM(CAST(total_price AS FLOAT64)) as revenue,
        AVG(CAST(total_price AS FLOAT64)) as avg_order_value,
        COUNT(DISTINCT JSON_VALUE(customer, '$.id')) as unique_customers,
        EXTRACT(DAYOFWEEK FROM created_at) as day_of_week
    FROM `artful-logic-475116-p1.raw_shopify.orders`
    WHERE cancelled_at IS NULL
        AND financial_status IN ('paid', 'partially_refunded')
        AND created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {lookback_days} DAY)
    GROUP BY order_date, day_of_week
    ORDER BY order_date
    """
    return client.query(query).to_dataframe()

@st.cache_data(ttl=300)
def load_product_sales(lookback_days: int = 30):
    client = get_bq_client()
    query = f"""
    WITH order_items AS (
        SELECT o.id as order_id, DATE(o.created_at) as order_date,
            JSON_VALUE(item, '$.title') as product_name, JSON_VALUE(item, '$.sku') as sku,
            CAST(JSON_VALUE(item, '$.quantity') AS INT64) as quantity,
            CAST(JSON_VALUE(item, '$.price') AS FLOAT64) as unit_price
        FROM `artful-logic-475116-p1.raw_shopify.orders` o,
        UNNEST(JSON_QUERY_ARRAY(o.line_items)) as item
        WHERE o.cancelled_at IS NULL AND o.financial_status IN ('paid', 'partially_refunded')
            AND o.created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {lookback_days} DAY)
    )
    SELECT product_name, sku, SUM(quantity) as units_sold, SUM(quantity * unit_price) as revenue,
        COUNT(DISTINCT order_id) as order_count, AVG(unit_price) as avg_price,
        ROUND(SUM(quantity) / {lookback_days} * 7, 1) as weekly_velocity
    FROM order_items
    WHERE product_name IS NOT NULL AND product_name NOT LIKE '%Shipping Protection%' AND product_name NOT LIKE '%Protectly%'
    GROUP BY product_name, sku HAVING units_sold > 0
    ORDER BY revenue DESC
    """
    return client.query(query).to_dataframe()

@st.cache_data(ttl=300)
def load_weekly_comparison():
    client = get_bq_client()
    query = """
    WITH weekly_data AS (
        SELECT DATE_TRUNC(DATE(created_at), WEEK(MONDAY)) as week_start,
            COUNT(DISTINCT id) as orders, SUM(CAST(total_price AS FLOAT64)) as revenue,
            AVG(CAST(total_price AS FLOAT64)) as aov
        FROM `artful-logic-475116-p1.raw_shopify.orders`
        WHERE cancelled_at IS NULL AND financial_status IN ('paid', 'partially_refunded')
            AND created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 84 DAY)
        GROUP BY week_start
    )
    SELECT *, LAG(revenue) OVER (ORDER BY week_start) as prev_week_revenue,
        LAG(orders) OVER (ORDER BY week_start) as prev_week_orders
    FROM weekly_data ORDER BY week_start
    """
    return client.query(query).to_dataframe()

@st.cache_data(ttl=300)
def load_hourly_pattern(lookback_days: int = 30):
    client = get_bq_client()
    query = f"""
    SELECT EXTRACT(HOUR FROM created_at) as hour_of_day,
        COUNT(DISTINCT id) as order_count, SUM(CAST(total_price AS FLOAT64)) as revenue
    FROM `artful-logic-475116-p1.raw_shopify.orders`
    WHERE cancelled_at IS NULL AND financial_status IN ('paid', 'partially_refunded')
        AND created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {lookback_days} DAY)
    GROUP BY hour_of_day ORDER BY hour_of_day
    """
    return client.query(query).to_dataframe()

def calculate_forecast(daily_df: pd.DataFrame, forecast_days: int = 30):
    if daily_df.empty or len(daily_df) < 14:
        return pd.DataFrame()
    df = daily_df.sort_values('order_date').copy()
    df['ma_7'] = df['revenue'].rolling(window=7, min_periods=1).mean()
    recent = df.tail(14)
    if len(recent) >= 2:
        x = np.arange(len(recent))
        slope, _ = np.polyfit(x, recent['revenue'].values, 1)
    else:
        slope = 0
    dow_avg = df.groupby('day_of_week')['revenue'].mean()
    dow_factors = (dow_avg / df['revenue'].mean()).to_dict()
    last_date, last_ma = df['order_date'].max(), df['ma_7'].iloc[-1]
    forecast_dates, forecast_revenue = [], []
    for i in range(1, forecast_days + 1):
        future_date = last_date + timedelta(days=i)
        dow_bq = future_date.isoweekday() % 7 + 1
        base = last_ma + (slope * i * 0.5)
        projected = max(0, base * dow_factors.get(dow_bq, 1.0))
        forecast_dates.append(future_date)
        forecast_revenue.append(projected)
    return pd.DataFrame({'order_date': forecast_dates, 'forecast_revenue': forecast_revenue})

def render_metric_card(value, label, card_type="primary", delta=None):
    value_class = {"warning": "metric-value-warning", "success": "metric-value-success", "info": "metric-value-info"}.get(card_type, "metric-value")
    delta_html = ""
    if delta is not None:
        delta_class = "metric-delta-positive" if delta >= 0 else "metric-delta-negative"
        delta_html = f'<div style="color: {"#64ffda" if delta >= 0 else "#ff6b6b"}; font-size: 14px;">{"↑" if delta >= 0 else "↓"} {abs(delta):.1f}%</div>'
    return f'<div class="metric-card"><div class="{value_class}">{value}</div><div class="metric-label">{label}</div>{delta_html}</div>'

# Header
st.markdown("""
<div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 32px;">
    <div>
        <h1 class="dashboard-header">Shopify Sales Forecast</h1>
        <p class="dashboard-subtitle">B2C Revenue Trends, Projections & Product Performance</p>
    </div>
    <div class="live-indicator"><span class="live-dot"></span> Live Data</div>
</div>
""", unsafe_allow_html=True)

# Filters
st.markdown('<div class="filter-container">', unsafe_allow_html=True)
col1, col2, col3 = st.columns([2, 1, 1])
with col1:
    lookback_days = st.selectbox("Historical Lookback", [30, 60, 90, 180], index=2, format_func=lambda x: f"{x} days")
with col2:
    forecast_days = st.selectbox("Forecast Period", [7, 14, 30], index=2, format_func=lambda x: f"{x} days")
with col3:
    st.markdown(f"**Data Through:** {datetime.now().strftime('%b %d, %Y')}")
st.markdown('</div>', unsafe_allow_html=True)

# Load data
try:
    daily_df = load_daily_sales(lookback_days)
    product_df = load_product_sales(min(lookback_days, 60))
    weekly_df = load_weekly_comparison()
    hourly_df = load_hourly_pattern(30)
except Exception as e:
    st.error(f"Error loading data: {e}")
    st.stop()

if daily_df.empty:
    st.warning("No sales data available.")
    st.stop()

# Metrics
total_revenue = daily_df['revenue'].sum()
total_orders = daily_df['order_count'].sum()
avg_daily_revenue = daily_df['revenue'].mean()
avg_order_value = total_revenue / total_orders if total_orders > 0 else 0

wow_revenue_change = wow_orders_change = 0
if len(weekly_df) >= 2:
    curr, prev = weekly_df.iloc[-1], weekly_df.iloc[-2]
    if prev['revenue'] > 0:
        wow_revenue_change = (curr['revenue'] - prev['revenue']) / prev['revenue'] * 100
    if prev['orders'] > 0:
        wow_orders_change = (curr['orders'] - prev['orders']) / prev['orders'] * 100

forecast_df = calculate_forecast(daily_df, forecast_days)
projected_revenue = forecast_df['forecast_revenue'].sum() if not forecast_df.empty else 0

# KPI Cards
col1, col2, col3, col4, col5 = st.columns(5)
with col1:
    st.markdown(render_metric_card(f"${total_revenue/1000:.0f}K", f"Revenue ({lookback_days}d)", delta=wow_revenue_change), unsafe_allow_html=True)
with col2:
    st.markdown(render_metric_card(f"{total_orders:,}", f"Orders ({lookback_days}d)", delta=wow_orders_change), unsafe_allow_html=True)
with col3:
    st.markdown(render_metric_card(f"${avg_order_value:.0f}", "Avg Order Value", card_type="info"), unsafe_allow_html=True)
with col4:
    st.markdown(render_metric_card(f"${avg_daily_revenue/1000:.1f}K", "Avg Daily Revenue", card_type="success"), unsafe_allow_html=True)
with col5:
    st.markdown(render_metric_card(f"${projected_revenue/1000:.0f}K", f"{forecast_days}d Forecast", card_type="warning"), unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# Revenue Trend Chart
st.markdown('<p class="section-header">Revenue Trend & Forecast</p>', unsafe_allow_html=True)
fig = go.Figure()
daily_sorted = daily_df.sort_values('order_date')
fig.add_trace(go.Scatter(x=daily_sorted['order_date'], y=daily_sorted['revenue'], mode='lines', name='Actual Revenue',
    line=dict(color=COLORS['primary'], width=2), fill='tozeroy', fillcolor='rgba(0, 212, 170, 0.1)'))
daily_sorted['ma_7'] = daily_sorted['revenue'].rolling(window=7, min_periods=1).mean()
fig.add_trace(go.Scatter(x=daily_sorted['order_date'], y=daily_sorted['ma_7'], mode='lines', name='7-Day Average',
    line=dict(color=COLORS['secondary'], width=3)))
if not forecast_df.empty:
    fig.add_trace(go.Scatter(x=forecast_df['order_date'], y=forecast_df['forecast_revenue'], mode='lines', name='Forecast',
        line=dict(color=COLORS['warning'], width=2, dash='dash'), fill='tozeroy', fillcolor='rgba(255, 214, 102, 0.1)'))
apply_dark_theme(fig, height=400, legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font=dict(color='#8892b0')),
    hovermode='x unified', yaxis={'title': 'Revenue ($)', 'titlefont': {'color': '#8892b0'}})
st.plotly_chart(fig, use_container_width=True)

if not forecast_df.empty:
    daily_avg_forecast = projected_revenue / forecast_days
    st.markdown(f"""
    <div class="forecast-banner">
        <strong style="color: #64ffda;">📈 {forecast_days}-Day Forecast:</strong>
        <span style="color: #ccd6f6; margin-left: 16px;">
            Projected: <strong>${projected_revenue:,.0f}</strong> |
            Daily Avg: <strong>${daily_avg_forecast:,.0f}</strong> |
            Monthly Run Rate: <strong>${daily_avg_forecast * 30:,.0f}</strong>
        </span>
    </div>
    """, unsafe_allow_html=True)

# Day of Week + Hourly
col1, col2 = st.columns(2)
with col1:
    st.markdown('<p class="section-header">Day of Week Performance</p>', unsafe_allow_html=True)
    dow_names = {1: 'Sun', 2: 'Mon', 3: 'Tue', 4: 'Wed', 5: 'Thu', 6: 'Fri', 7: 'Sat'}
    dow_data = daily_df.groupby('day_of_week')['revenue'].mean().reset_index()
    dow_data['day_name'] = dow_data['day_of_week'].map(dow_names)
    fig = go.Figure(go.Bar(x=dow_data['day_name'], y=dow_data['revenue'],
        marker=dict(color=dow_data['revenue'], colorscale=[[0, COLORS['secondary']], [1, COLORS['primary']]]),
        text=dow_data['revenue'].apply(lambda x: f'${x/1000:.1f}K'), textposition='outside', textfont=dict(color='#ccd6f6')))
    apply_dark_theme(fig, height=300, yaxis={'title': 'Avg Daily Revenue'})
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.markdown('<p class="section-header">Hourly Sales Pattern</p>', unsafe_allow_html=True)
    if not hourly_df.empty:
        fig = go.Figure(go.Scatter(x=hourly_df['hour_of_day'], y=hourly_df['order_count'], mode='lines+markers', name='Orders',
            line=dict(color=COLORS['info'], width=3), marker=dict(size=8), fill='tozeroy', fillcolor='rgba(116, 185, 255, 0.1)'))
        apply_dark_theme(fig, height=300, xaxis={'title': 'Hour (UTC)', 'dtick': 3}, yaxis={'title': 'Order Count'})
        st.plotly_chart(fig, use_container_width=True)

# Weekly + Top Products
col1, col2 = st.columns(2)
with col1:
    st.markdown('<p class="section-header">Weekly Revenue Trend</p>', unsafe_allow_html=True)
    if not weekly_df.empty:
        fig = go.Figure()
        fig.add_trace(go.Bar(x=weekly_df['week_start'], y=weekly_df['revenue'], name='Weekly Revenue',
            marker=dict(color=COLORS['primary']), text=weekly_df['revenue'].apply(lambda x: f'${x/1000:.0f}K'),
            textposition='outside', textfont=dict(color='#ccd6f6')))
        apply_dark_theme(fig, height=300)
        st.plotly_chart(fig, use_container_width=True)

with col2:
    st.markdown('<p class="section-header">Top Products by Revenue</p>', unsafe_allow_html=True)
    if not product_df.empty:
        top_products = product_df.nlargest(8, 'revenue')
        fig = go.Figure(go.Bar(x=top_products['revenue'], y=top_products['product_name'], orientation='h',
            marker=dict(color=top_products['revenue'], colorscale=[[0, COLORS['secondary']], [1, COLORS['primary']]]),
            text=top_products['revenue'].apply(lambda x: f'${x/1000:.1f}K'), textposition='outside', textfont=dict(color='#ccd6f6')))
        apply_dark_theme(fig, height=300, margin=dict(l=0, r=60, t=10, b=0), yaxis={'autorange': 'reversed'})
        st.plotly_chart(fig, use_container_width=True)

# Product Table
st.markdown('<p class="section-header">Product Performance Summary</p>', unsafe_allow_html=True)
if not product_df.empty:
    display_df = product_df.head(20).copy()
    display_df['revenue'] = display_df['revenue'].apply(lambda x: f"${x:,.0f}")
    display_df['avg_price'] = display_df['avg_price'].apply(lambda x: f"${x:.2f}")
    display_df['weekly_velocity'] = display_df['weekly_velocity'].apply(lambda x: f"{x:.1f}/wk")
    display_df = display_df[['product_name', 'units_sold', 'revenue', 'order_count', 'avg_price', 'weekly_velocity']]
    display_df.columns = ['Product', 'Units Sold', 'Revenue', 'Orders', 'Avg Price', 'Weekly Velocity']
    st.dataframe(display_df, use_container_width=True, hide_index=True, height=400)

# Footer
st.markdown(f"""
<div style="text-align: center; color: #8892b0; margin-top: 48px; padding: 24px; border-top: 1px solid rgba(255,255,255,0.1);">
    <p>Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} UTC | Lookback: {lookback_days}d | Forecast: {forecast_days}d</p>
</div>
""", unsafe_allow_html=True)
