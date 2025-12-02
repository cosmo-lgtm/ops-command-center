"""
ShipStation Fulfillment Dashboard
B2B order fulfillment analytics with dark mode theme
"""

import streamlit as st
import pandas as pd
from google.cloud import bigquery
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go

# Page config - MUST be first Streamlit command
st.set_page_config(
    page_title="ShipStation Fulfillment",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Dark mode custom CSS
st.markdown("""
<style>
    /* Force wide layout */
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
    header {visibility: hidden;}

    /* Base styles (mobile-first) */
    .metric-card {
        background: linear-gradient(145deg, #1e1e2f 0%, #2a2a4a 100%);
        border-radius: 12px;
        padding: 16px;
        border: 1px solid rgba(255,255,255,0.1);
        box-shadow: 0 8px 32px rgba(0,0,0,0.3);
        margin-bottom: 12px;
    }
    .metric-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 12px 40px rgba(0,0,0,0.4);
    }
    .metric-value { font-size: clamp(1.5rem, 4vw, 2.625rem); font-weight: 700; background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%); -webkit-background-clip: text; -webkit-text-fill-color: transparent; margin: 0; }
    .metric-label { font-size: clamp(0.7rem, 1.5vw, 0.875rem); color: #8892b0; text-transform: uppercase; letter-spacing: 1px; margin-top: 6px; }
    .metric-delta-positive { color: #64ffda; font-size: clamp(0.65rem, 1.2vw, 0.875rem); }
    .metric-delta-negative { color: #ff6b6b; font-size: clamp(0.65rem, 1.2vw, 0.875rem); }

    .dashboard-header { background: linear-gradient(90deg, #f093fb 0%, #f5576c 100%); -webkit-background-clip: text; -webkit-text-fill-color: transparent; font-size: clamp(1.75rem, 5vw, 3rem); font-weight: 800; margin-bottom: 8px; }
    .dashboard-subtitle { color: #8892b0; font-size: clamp(0.875rem, 2vw, 1rem); margin-bottom: 24px; }
    .section-header { color: #ccd6f6; font-size: clamp(1.1rem, 2.5vw, 1.5rem); font-weight: 600; margin: 24px 0 12px 0; padding-bottom: 8px; border-bottom: 2px solid rgba(240, 147, 251, 0.3); }

    /* Status badges - responsive */
    .status-healthy { background: rgba(100, 255, 218, 0.2); color: #64ffda; padding: 3px 10px; border-radius: 20px; font-size: clamp(0.65rem, 1.2vw, 0.75rem); font-weight: 600; }
    .status-warning { background: rgba(255, 214, 102, 0.2); color: #ffd666; padding: 3px 10px; border-radius: 20px; font-size: clamp(0.65rem, 1.2vw, 0.75rem); font-weight: 600; }
    .status-critical { background: rgba(255, 107, 107, 0.2); color: #ff6b6b; padding: 3px 10px; border-radius: 20px; font-size: clamp(0.65rem, 1.2vw, 0.75rem); font-weight: 600; }

    .live-indicator { display: inline-flex; align-items: center; gap: 8px; color: #64ffda; font-size: clamp(0.65rem, 1.5vw, 0.75rem); text-transform: uppercase; letter-spacing: 1px; }
    .live-dot { width: 8px; height: 8px; background: #64ffda; border-radius: 50%; animation: pulse 2s infinite; }
    @keyframes pulse { 0%, 100% { opacity: 1; transform: scale(1); } 50% { opacity: 0.5; transform: scale(1.2); } }

    /* Scrollbar */
    ::-webkit-scrollbar { width: 8px; height: 8px; }
    ::-webkit-scrollbar-track { background: #1a1a2e; }
    ::-webkit-scrollbar-thumb { background: #f093fb; border-radius: 4px; }

    /* Tablet breakpoint */
    @media (max-width: 992px) {
        .block-container {
            padding-left: 1rem !important;
            padding-right: 1rem !important;
        }
    }

    /* Mobile breakpoint */
    @media (max-width: 640px) {
        .block-container {
            padding-left: 0.5rem !important;
            padding-right: 0.5rem !important;
        }
        .metric-card {
            padding: 12px;
            border-radius: 8px;
        }
    }
</style>
""", unsafe_allow_html=True)

COLORS = {
    'primary': '#f093fb',
    'secondary': '#f5576c',
    'success': '#64ffda',
    'warning': '#ffd666',
    'danger': '#ff6b6b',
    'info': '#74b9ff',
    'gradient': ['#f093fb', '#f5576c', '#667eea', '#764ba2']
}


def apply_dark_theme(fig, height=350, **kwargs):
    """Apply dark theme to a plotly figure."""
    layout_args = {
        'paper_bgcolor': 'rgba(0,0,0,0)',
        'plot_bgcolor': 'rgba(0,0,0,0)',
        'font': {'color': '#ccd6f6', 'family': 'Inter, sans-serif'},
        'height': height,
        'margin': kwargs.get('margin', dict(l=0, r=0, t=20, b=0)),
        'xaxis': {
            'gridcolor': 'rgba(255,255,255,0.1)',
            'linecolor': 'rgba(255,255,255,0.1)',
            'tickfont': {'color': '#8892b0'},
            **kwargs.get('xaxis', {})
        },
        'yaxis': {
            'gridcolor': 'rgba(255,255,255,0.1)',
            'linecolor': 'rgba(255,255,255,0.1)',
            'tickfont': {'color': '#8892b0'},
            **kwargs.get('yaxis', {})
        }
    }
    for k, v in kwargs.items():
        if k not in ['xaxis', 'yaxis', 'margin']:
            layout_args[k] = v
    fig.update_layout(**layout_args)
    return fig


@st.cache_resource
def get_bq_client():
    """Initialize BigQuery client."""
    try:
        if "gcp_service_account" in st.secrets:
            from google.oauth2 import service_account
            credentials = service_account.Credentials.from_service_account_info(
                st.secrets["gcp_service_account"]
            )
            return bigquery.Client(project='artful-logic-475116-p1', credentials=credentials)
    except Exception:
        pass
    # Fall back to default credentials (local dev with gcloud auth)
    return bigquery.Client(project='artful-logic-475116-p1')


@st.cache_data(ttl=300)
def load_daily_metrics():
    """Load daily fulfillment metrics from BigQuery."""
    client = get_bq_client()
    query = """
    SELECT *
    FROM `artful-logic-475116-p1.mart_shipstation.dim_daily_fulfillment`
    ORDER BY order_date DESC
    LIMIT 90
    """
    return client.query(query).to_dataframe()


@st.cache_data(ttl=300)
def load_carrier_performance():
    """Load carrier performance metrics."""
    client = get_bq_client()
    query = """
    SELECT *
    FROM `artful-logic-475116-p1.mart_shipstation.dim_carrier_performance`
    WHERE order_month >= DATE_TRUNC(CURRENT_DATE(), MONTH) - INTERVAL 3 MONTH
    ORDER BY order_month DESC, order_count DESC
    """
    return client.query(query).to_dataframe()


@st.cache_data(ttl=300)
def load_state_distribution():
    """Load state distribution metrics."""
    client = get_bq_client()
    query = """
    SELECT *
    FROM `artful-logic-475116-p1.mart_shipstation.dim_state_distribution`
    WHERE ship_country = 'US'
    ORDER BY order_count DESC
    LIMIT 20
    """
    return client.query(query).to_dataframe()


@st.cache_data(ttl=300)
def load_current_stats():
    """Load current summary statistics."""
    client = get_bq_client()
    query = """
    WITH current_month AS (
      SELECT
        COUNT(DISTINCT orderId) as orders_this_month,
        SUM(orderTotal) as revenue_this_month,
        SUM(shipmentCost) as shipping_this_month,
        COUNTIF(fulfillment_status = 'shipped') as shipped_this_month,
        COUNTIF(fulfillment_status = 'pending') as pending_this_month,
        ROUND(100.0 * COUNTIF(fulfillment_status = 'shipped') / NULLIF(COUNT(*), 0), 1) as fulfillment_rate,
        ROUND(AVG(CASE WHEN fulfillment_status = 'shipped' THEN days_to_ship END), 1) as avg_days_to_ship
      FROM `artful-logic-475116-p1.mart_shipstation.fct_order_shipment`
      WHERE order_date >= DATE_TRUNC(CURRENT_DATE(), MONTH)
    ),
    last_month AS (
      SELECT
        COUNT(DISTINCT orderId) as orders_last_month,
        SUM(orderTotal) as revenue_last_month,
        SUM(shipmentCost) as shipping_last_month
      FROM `artful-logic-475116-p1.mart_shipstation.fct_order_shipment`
      WHERE order_date >= DATE_TRUNC(CURRENT_DATE() - INTERVAL 1 MONTH, MONTH)
        AND order_date < DATE_TRUNC(CURRENT_DATE(), MONTH)
    ),
    today_stats AS (
      SELECT
        COUNT(DISTINCT orderId) as orders_today,
        COUNTIF(fulfillment_status = 'shipped') as shipped_today
      FROM `artful-logic-475116-p1.mart_shipstation.fct_order_shipment`
      WHERE order_date = CURRENT_DATE()
    )
    SELECT *
    FROM current_month, last_month, today_stats
    """
    return client.query(query).to_dataframe().iloc[0]


@st.cache_data(ttl=300)
def load_recent_orders():
    """Load recent orders for detail table."""
    client = get_bq_client()
    query = """
    SELECT
      orderNumber,
      order_date,
      fulfillment_status,
      orderTotal,
      COALESCE(shipment_carrier, order_carrier, 'N/A') as carrier,
      ship_state,
      trackingNumber
    FROM `artful-logic-475116-p1.mart_shipstation.fct_order_shipment`
    WHERE order_date >= CURRENT_DATE() - 7
    ORDER BY order_date DESC, orderId DESC
    LIMIT 25
    """
    return client.query(query).to_dataframe()


def render_metric_card(value, label, delta=None, delta_type="positive"):
    """Render a styled metric card."""
    delta_html = ""
    if delta:
        delta_class = "metric-delta-positive" if delta_type == "positive" else "metric-delta-negative"
        delta_symbol = "+" if delta_type == "positive" else ""
        delta_html = f'<div class="{delta_class}">{delta_symbol}{delta}</div>'

    return f"""
    <div class="metric-card">
        <div class="metric-value">{value}</div>
        <div class="metric-label">{label}</div>
        {delta_html}
    </div>
    """


def main():
    # Header
    st.markdown("""
    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 32px;">
        <div>
            <h1 class="dashboard-header">Fulfillment Command Center</h1>
            <p class="dashboard-subtitle">ShipStation B2B Order Analytics</p>
        </div>
        <div class="live-indicator">
            <span class="live-dot"></span>
            Live Data
        </div>
    </div>
    """, unsafe_allow_html=True)

    # Load data
    try:
        stats = load_current_stats()
        daily_df = load_daily_metrics()
        carrier_df = load_carrier_performance()
        state_df = load_state_distribution()
        recent_orders = load_recent_orders()
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return

    # KPI Cards Row
    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        order_delta = stats['orders_this_month'] - stats['orders_last_month']
        delta_pct = round(100 * order_delta / max(stats['orders_last_month'], 1), 1)
        st.markdown(render_metric_card(
            f"{stats['orders_this_month']:,.0f}",
            "Orders This Month",
            f"{delta_pct}% vs last month",
            "positive" if order_delta >= 0 else "negative"
        ), unsafe_allow_html=True)

    with col2:
        st.markdown(render_metric_card(
            f"{stats['fulfillment_rate']:.1f}%",
            "Fulfillment Rate",
            delta_type="positive" if stats['fulfillment_rate'] > 85 else "negative"
        ), unsafe_allow_html=True)

    with col3:
        days_display = stats['avg_days_to_ship'] if pd.notna(stats['avg_days_to_ship']) else 0
        st.markdown(render_metric_card(
            f"{days_display:.1f}",
            "Avg Days to Ship"
        ), unsafe_allow_html=True)

    with col4:
        st.markdown(render_metric_card(
            f"{stats['pending_this_month']:,.0f}",
            "Pending Orders",
            delta_type="negative" if stats['pending_this_month'] > 50 else "positive"
        ), unsafe_allow_html=True)

    with col5:
        shipping_k = stats['shipping_this_month'] / 1000 if pd.notna(stats['shipping_this_month']) else 0
        st.markdown(render_metric_card(
            f"${shipping_k:,.1f}K",
            "Shipping Cost MTD"
        ), unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # Charts Row 1: Volume Trend + Fulfillment Rate
    col1, col2 = st.columns([2, 1])

    with col1:
        st.markdown('<p class="section-header">Order Volume Trend</p>', unsafe_allow_html=True)

        daily_sorted = daily_df.sort_values('order_date')

        fig = go.Figure()

        # Stacked area: shipped vs pending
        fig.add_trace(go.Scatter(
            x=daily_sorted['order_date'],
            y=daily_sorted['orders_shipped'],
            mode='lines',
            name='Shipped',
            line=dict(color=COLORS['success'], width=2),
            fill='tozeroy',
            fillcolor='rgba(100, 255, 218, 0.3)'
        ))

        fig.add_trace(go.Scatter(
            x=daily_sorted['order_date'],
            y=daily_sorted['orders_placed'],
            mode='lines',
            name='Total Orders',
            line=dict(color=COLORS['primary'], width=3)
        ))

        # 7-day moving average
        daily_sorted['ma7'] = daily_sorted['orders_placed'].rolling(7).mean()
        fig.add_trace(go.Scatter(
            x=daily_sorted['order_date'],
            y=daily_sorted['ma7'],
            mode='lines',
            name='7-day Avg',
            line=dict(color=COLORS['secondary'], width=2, dash='dot')
        ))

        apply_dark_theme(fig, height=350,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font=dict(color='#8892b0')),
            hovermode='x unified'
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown('<p class="section-header">Fulfillment Rate</p>', unsafe_allow_html=True)

        daily_sorted = daily_df.sort_values('order_date')

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=daily_sorted['order_date'],
            y=daily_sorted['fulfillment_rate'],
            mode='lines+markers',
            name='Fulfillment %',
            line=dict(color=COLORS['success'], width=2),
            marker=dict(size=4)
        ))

        # Target line at 90%
        fig.add_hline(y=90, line_dash="dash", line_color=COLORS['warning'],
                      annotation_text="Target: 90%", annotation_position="right")

        apply_dark_theme(fig, height=350, showlegend=False, yaxis={'range': [0, 105]})
        st.plotly_chart(fig, use_container_width=True)

    # Charts Row 2: Carrier Mix + State Distribution
    col1, col2 = st.columns(2)

    with col1:
        st.markdown('<p class="section-header">Carrier Mix (Current Month)</p>', unsafe_allow_html=True)

        # Get current month carrier data
        current_month = carrier_df[carrier_df['order_month'] == carrier_df['order_month'].max()].copy()
        current_month = current_month[current_month['carrier_code'].notna()]

        if not current_month.empty:
            # Clean up carrier names
            carrier_names = {
                'ups_walleted': 'UPS',
                'ups': 'UPS Direct',
                'stamps_com': 'Stamps.com',
                'fedex': 'FedEx',
                'globalpost': 'GlobalPost'
            }
            current_month['carrier_display'] = current_month['carrier_code'].map(
                lambda x: carrier_names.get(x, x.replace('_', ' ').title())
            )

            fig = go.Figure(data=[go.Pie(
                labels=current_month['carrier_display'],
                values=current_month['order_count'],
                hole=0.5,
                marker=dict(colors=COLORS['gradient']),
                textinfo='label+percent',
                textposition='outside',
                textfont=dict(color='#ccd6f6')
            )])

            apply_dark_theme(fig, height=350, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No carrier data available for current month")

    with col2:
        st.markdown('<p class="section-header">Top States by Orders</p>', unsafe_allow_html=True)

        top_states = state_df.head(10)

        fig = go.Figure(go.Bar(
            x=top_states['order_count'],
            y=top_states['ship_state'],
            orientation='h',
            marker=dict(
                color=top_states['order_count'],
                colorscale=[[0, COLORS['primary']], [1, COLORS['secondary']]],
            ),
            hovertemplate='%{y}<br>Orders: %{x:,}<extra></extra>'
        ))

        apply_dark_theme(fig, height=350, margin=dict(l=0, r=0, t=10, b=0), yaxis={'autorange': 'reversed'})
        st.plotly_chart(fig, use_container_width=True)

    # Charts Row 3: Shipping Cost + Order Status
    col1, col2 = st.columns(2)

    with col1:
        st.markdown('<p class="section-header">Avg Shipping Cost by Carrier</p>', unsafe_allow_html=True)

        current_month = carrier_df[carrier_df['order_month'] == carrier_df['order_month'].max()].copy()
        current_month = current_month[current_month['carrier_code'].notna() & current_month['avg_shipping_cost'].notna()]

        if not current_month.empty:
            carrier_names = {
                'ups_walleted': 'UPS',
                'ups': 'UPS Direct',
                'stamps_com': 'Stamps.com',
                'fedex': 'FedEx',
                'globalpost': 'GlobalPost'
            }
            current_month['carrier_display'] = current_month['carrier_code'].map(
                lambda x: carrier_names.get(x, x.replace('_', ' ').title())
            )

            fig = go.Figure(go.Bar(
                x=current_month['carrier_display'],
                y=current_month['avg_shipping_cost'],
                marker_color=COLORS['info'],
                text=current_month['avg_shipping_cost'].apply(lambda x: f'${x:.2f}'),
                textposition='outside',
                textfont=dict(color='#ccd6f6'),
                hovertemplate='%{x}<br>Avg Cost: $%{y:.2f}<extra></extra>'
            ))

            apply_dark_theme(fig, height=300, xaxis={'tickangle': 0})
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No shipping cost data available")

    with col2:
        st.markdown('<p class="section-header">Order Status (This Month)</p>', unsafe_allow_html=True)

        status_data = pd.DataFrame({
            'Status': ['Shipped', 'Pending', 'Cancelled'],
            'Count': [
                stats['shipped_this_month'],
                stats['pending_this_month'],
                stats['orders_this_month'] - stats['shipped_this_month'] - stats['pending_this_month']
            ]
        })
        status_data = status_data[status_data['Count'] > 0]

        fig = go.Figure(data=[go.Pie(
            labels=status_data['Status'],
            values=status_data['Count'],
            hole=0.6,
            marker=dict(colors=[COLORS['success'], COLORS['warning'], COLORS['danger']]),
            textinfo='label+value',
            textposition='outside',
            textfont=dict(color='#ccd6f6')
        )])

        apply_dark_theme(fig, height=300, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    # Recent Orders Table
    st.markdown('<p class="section-header">Recent Orders (Last 7 Days)</p>', unsafe_allow_html=True)

    if not recent_orders.empty:
        # Format the dataframe for display
        display_df = recent_orders.copy()
        display_df['orderTotal'] = display_df['orderTotal'].apply(lambda x: f"${x:,.2f}" if pd.notna(x) else "N/A")
        display_df.columns = ['Order #', 'Date', 'Status', 'Total', 'Carrier', 'State', 'Tracking']

        # Add status badges
        def style_status(val):
            if val == 'shipped':
                return 'background-color: rgba(100, 255, 218, 0.2); color: #64ffda'
            elif val == 'pending':
                return 'background-color: rgba(255, 214, 102, 0.2); color: #ffd666'
            else:
                return 'background-color: rgba(255, 107, 107, 0.2); color: #ff6b6b'

        st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True,
            height=400
        )
    else:
        st.info("No recent orders found")

    # Footer
    st.markdown(f"""
    <div style="text-align: center; color: #8892b0; margin-top: 48px; padding: 24px; border-top: 1px solid rgba(255,255,255,0.1);">
        <p style="margin: 0;">Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} UTC</p>
        <p style="margin: 4px 0 0 0; font-size: 12px;">Data refreshes every 5 minutes</p>
    </div>
    """, unsafe_allow_html=True)


if __name__ == "__main__":
    main()
