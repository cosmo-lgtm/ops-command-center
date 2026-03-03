"""
DTC Operations Dashboard
CEO-level metrics for Shopify, Zendesk, and Kase operations
"""

import streamlit as st
import pandas as pd
from google.cloud import bigquery
from datetime import datetime
import plotly.graph_objects as go

# Dark mode CSS
st.markdown("""
<style>
    .metric-card {
        background: linear-gradient(145deg, #1e1e2f 0%, #2a2a4a 100%);
        border-radius: 16px;
        padding: 24px;
        border: 1px solid rgba(255,255,255,0.1);
        box-shadow: 0 8px 32px rgba(0,0,0,0.3);
    }

    .metric-value {
        font-size: 36px;
        font-weight: 700;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
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
        font-size: 20px;
        font-weight: 600;
        margin: 24px 0 12px 0;
        padding-bottom: 8px;
        border-bottom: 2px solid rgba(102, 126, 234, 0.3);
    }

    .source-label {
        color: #5a6a8a;
        font-size: 11px;
        font-style: italic;
        margin-top: 4px;
    }

    .coming-soon {
        background: linear-gradient(145deg, #1e1e2f 0%, #2a2a4a 100%);
        border-radius: 16px;
        padding: 60px 24px;
        border: 1px solid rgba(255,255,255,0.1);
        text-align: center;
    }

    .coming-soon-text {
        color: #5a6a8a;
        font-size: 18px;
        font-weight: 500;
    }

    .coming-soon-icon {
        font-size: 48px;
        margin-bottom: 16px;
    }
</style>
""", unsafe_allow_html=True)

COLORS = {
    'primary': '#667eea',
    'secondary': '#764ba2',
    'success': '#64ffda',
    'warning': '#ffd666',
    'info': '#74b9ff',
    'cs_reship': '#667eea',
    'protectly': '#f093fb'
}


def apply_dark_theme(fig, height=350, **kwargs):
    """Apply dark theme to plotly figure."""
    layout_args = {
        'paper_bgcolor': 'rgba(0,0,0,0)',
        'plot_bgcolor': 'rgba(0,0,0,0)',
        'font': {'color': '#ccd6f6', 'family': 'Inter, sans-serif'},
        'height': height,
        'margin': kwargs.get('margin', dict(l=40, r=20, t=40, b=40)),
        'xaxis': {
            'gridcolor': 'rgba(255,255,255,0.05)',
            'linecolor': 'rgba(255,255,255,0.1)',
            'tickfont': {'color': '#8892b0'},
            **kwargs.get('xaxis', {})
        },
        'yaxis': {
            'gridcolor': 'rgba(255,255,255,0.05)',
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
        if hasattr(st, 'secrets') and "gcp_service_account" in st.secrets:
            from google.oauth2 import service_account
            credentials = service_account.Credentials.from_service_account_info(
                st.secrets["gcp_service_account"]
            )
            return bigquery.Client(project='artful-logic-475116-p1', credentials=credentials)
    except Exception:
        pass
    return bigquery.Client(project='artful-logic-475116-p1')


@st.cache_data(ttl=300)
def load_monthly_tickets():
    """Load monthly CS ticket totals."""
    client = get_bq_client()
    query = """
    SELECT
        FORMAT_DATE('%Y-%m', created_date) as month,
        SUM(ticket_count) as total_tickets
    FROM `artful-logic-475116-p1.mart_zendesk.dim_daily_metrics`
    WHERE created_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)
    GROUP BY 1
    ORDER BY 1
    """
    return client.query(query).to_dataframe()


@st.cache_data(ttl=300)
def load_tag_metrics():
    """Load monthly tag-based metrics."""
    client = get_bq_client()
    query = """
    SELECT
        FORMAT_DATE('%Y-%m', created_month) as month,
        tag,
        SUM(ticket_count) as count
    FROM `artful-logic-475116-p1.mart_zendesk.dim_tag_analysis`
    WHERE tag IN ('address_change', 'wrongaddress', 'order_status', 'reship', 'protectly', 'reshipment')
      AND created_month >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)
    GROUP BY 1, 2
    ORDER BY 1, 2
    """
    return client.query(query).to_dataframe()


def format_month_label(month_str):
    """Convert 2024-01 to Jan 24 format."""
    try:
        dt = datetime.strptime(month_str, '%Y-%m')
        return dt.strftime("%b '%y")
    except:
        return month_str


def render_metric_card(value, label):
    """Render a styled metric card."""
    return f"""
    <div class="metric-card">
        <div class="metric-value">{value}</div>
        <div class="metric-label">{label}</div>
    </div>
    """


def render_coming_soon(title):
    """Render a coming soon placeholder."""
    return f"""
    <div class="coming-soon">
        <div class="coming-soon-icon">🚧</div>
        <div class="coming-soon-text">{title}<br>Coming Soon</div>
        <div class="source-label">Source: Kase (not yet integrated)</div>
    </div>
    """


# Header
st.markdown("""
<div style="margin-bottom: 32px;">
    <h1 class="dashboard-header">DTC Operations Dashboard</h1>
    <p class="dashboard-subtitle">CEO Metrics • Rolling 12-Month View • Shopify + Zendesk</p>
</div>
""", unsafe_allow_html=True)

# Load data
try:
    tickets_df = load_monthly_tickets()
    tags_df = load_tag_metrics()
except Exception as e:
    st.error(f"Error loading data: {e}")
    st.stop()

# Prepare tag data pivots
def get_tag_series(tags_df, tag_names, month_col='month'):
    """Sum counts for given tags by month."""
    if isinstance(tag_names, str):
        tag_names = [tag_names]
    filtered = tags_df[tags_df['tag'].isin(tag_names)]
    return filtered.groupby(month_col)['count'].sum().reset_index()

# Get all months for consistent x-axis
all_months = sorted(tickets_df['month'].unique())
month_labels = [format_month_label(m) for m in all_months]

# ----- ROW 1: Transit Time + CS Total Tickets -----
st.markdown('<p class="section-header">📦 Fulfillment & Support Volume</p>', unsafe_allow_html=True)

col1, col2 = st.columns(2)

with col1:
    st.markdown("**Transit Time (Days)**")
    st.markdown(render_coming_soon("Transit Time Data"), unsafe_allow_html=True)

with col2:
    st.markdown("**CS Total Tickets**")

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=month_labels,
        y=tickets_df['total_tickets'],
        marker_color=COLORS['primary'],
        hovertemplate='%{x}<br>Tickets: %{y:,}<extra></extra>'
    ))
    apply_dark_theme(fig, height=300)
    st.plotly_chart(fig, use_container_width=True)
    st.markdown('<p class="source-label">Source: Zendesk</p>', unsafe_allow_html=True)

# ----- ROW 2: Reships (CS + Protectly stacked) -----
st.markdown('<p class="section-header">🔄 Reshipments</p>', unsafe_allow_html=True)

# Get reship data
cs_reships = get_tag_series(tags_df, ['reship', 'reshipment'])
protectly_reships = get_tag_series(tags_df, 'protectly')

# Align to all months
cs_reships_aligned = pd.DataFrame({'month': all_months}).merge(
    cs_reships, on='month', how='left'
).fillna(0)
protectly_aligned = pd.DataFrame({'month': all_months}).merge(
    protectly_reships, on='month', how='left'
).fillna(0)

fig = go.Figure()
fig.add_trace(go.Bar(
    name='CS Reships',
    x=month_labels,
    y=cs_reships_aligned['count'],
    marker_color=COLORS['cs_reship'],
    hovertemplate='%{x}<br>CS Reships: %{y:,.0f}<extra></extra>'
))
fig.add_trace(go.Bar(
    name='Protectly Reships',
    x=month_labels,
    y=protectly_aligned['count'],
    marker_color=COLORS['protectly'],
    hovertemplate='%{x}<br>Protectly: %{y:,.0f}<extra></extra>'
))
fig.update_layout(barmode='group')
apply_dark_theme(fig, height=300,
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font=dict(color='#8892b0'))
)
st.plotly_chart(fig, use_container_width=True)
st.markdown('<p class="source-label">Source: Zendesk tags (reship, protectly)</p>', unsafe_allow_html=True)

# ----- ROW 3: Address Changes + Order Status -----
st.markdown('<p class="section-header">📋 Customer Request Types</p>', unsafe_allow_html=True)

col1, col2 = st.columns(2)

with col1:
    st.markdown("**Address Change Requests**")

    # Combine address_change and wrongaddress tags
    address_changes = get_tag_series(tags_df, ['address_change', 'wrongaddress'])
    address_aligned = pd.DataFrame({'month': all_months}).merge(
        address_changes, on='month', how='left'
    ).fillna(0)

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=month_labels,
        y=address_aligned['count'],
        marker_color=COLORS['warning'],
        hovertemplate='%{x}<br>Address Changes: %{y:,.0f}<extra></extra>'
    ))
    apply_dark_theme(fig, height=280)
    st.plotly_chart(fig, use_container_width=True)
    st.markdown('<p class="source-label">Source: Zendesk tags</p>', unsafe_allow_html=True)

with col2:
    st.markdown("**Order Status Requests**")

    order_status = get_tag_series(tags_df, 'order_status')
    order_aligned = pd.DataFrame({'month': all_months}).merge(
        order_status, on='month', how='left'
    ).fillna(0)

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=month_labels,
        y=order_aligned['count'],
        marker_color=COLORS['info'],
        hovertemplate='%{x}<br>Order Status: %{y:,.0f}<extra></extra>'
    ))
    apply_dark_theme(fig, height=280)
    st.plotly_chart(fig, use_container_width=True)
    st.markdown('<p class="source-label">Source: Zendesk tags</p>', unsafe_allow_html=True)

# ----- Summary KPIs -----
st.markdown('<p class="section-header">📈 Current Month Summary</p>', unsafe_allow_html=True)

# Get current month data
current_month = all_months[-1] if all_months else None
if current_month:
    current_tickets = tickets_df[tickets_df['month'] == current_month]['total_tickets'].sum()
    current_reships = cs_reships_aligned[cs_reships_aligned['month'] == current_month]['count'].sum()
    current_protectly = protectly_aligned[protectly_aligned['month'] == current_month]['count'].sum()
    current_address = address_aligned[address_aligned['month'] == current_month]['count'].sum()

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.markdown(render_metric_card(f"{current_tickets:,.0f}", "Total Tickets"), unsafe_allow_html=True)
    with col2:
        st.markdown(render_metric_card(f"{current_reships:,.0f}", "CS Reships"), unsafe_allow_html=True)
    with col3:
        st.markdown(render_metric_card(f"{current_protectly:,.0f}", "Protectly Reships"), unsafe_allow_html=True)
    with col4:
        st.markdown(render_metric_card(f"{current_address:,.0f}", "Address Changes"), unsafe_allow_html=True)

# Footer
st.markdown(f"""
<div style="text-align: center; color: #5a6a8a; margin-top: 48px; padding: 24px; border-top: 1px solid rgba(255,255,255,0.1);">
    <p style="margin: 0;">Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M')} UTC</p>
    <p style="margin: 4px 0 0 0; font-size: 12px;">Data refreshes every 5 minutes</p>
</div>
""", unsafe_allow_html=True)
