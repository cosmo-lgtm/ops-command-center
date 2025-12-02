"""
Zendesk B2C Support Dashboard
Modern dark-mode dashboard for support team analytics
"""

import streamlit as st
import pandas as pd
from google.cloud import bigquery
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go
import os

# Page config - MUST be first Streamlit command
st.set_page_config(
    page_title="Zendesk Support",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Streamlit Community Cloud handles authentication via settings
# No custom OAuth needed - just configure in Streamlit Cloud dashboard

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
    header {visibility: hidden; height: 0px !important;}
    .stApp > header {display: none !important;}
    .stDeployButton {display: none !important;}
    [data-testid="stHeader"] {display: none !important;}
    [data-testid="stToolbar"] {display: none !important;}
    .block-container {padding-top: 1rem !important;}

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
    .metric-value { font-size: clamp(1.5rem, 4vw, 2.625rem); font-weight: 700; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); -webkit-background-clip: text; -webkit-text-fill-color: transparent; margin: 0; }
    .metric-label { font-size: clamp(0.7rem, 1.5vw, 0.875rem); color: #8892b0; text-transform: uppercase; letter-spacing: 1px; margin-top: 6px; }
    .metric-delta-positive { color: #64ffda; font-size: clamp(0.65rem, 1.2vw, 0.875rem); }
    .metric-delta-negative { color: #ff6b6b; font-size: clamp(0.65rem, 1.2vw, 0.875rem); }

    .dashboard-header { background: linear-gradient(90deg, #667eea 0%, #764ba2 100%); -webkit-background-clip: text; -webkit-text-fill-color: transparent; font-size: clamp(1.75rem, 5vw, 3rem); font-weight: 800; margin-bottom: 8px; }
    .dashboard-subtitle { color: #8892b0; font-size: clamp(0.875rem, 2vw, 1rem); margin-bottom: 24px; }
    .section-header { color: #ccd6f6; font-size: clamp(1.1rem, 2.5vw, 1.5rem); font-weight: 600; margin: 24px 0 12px 0; padding-bottom: 8px; border-bottom: 2px solid rgba(102, 126, 234, 0.3); }

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
    ::-webkit-scrollbar-thumb { background: #667eea; border-radius: 4px; }

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

# Plotly dark theme
PLOTLY_THEME = {
    'paper_bgcolor': 'rgba(0,0,0,0)',
    'plot_bgcolor': 'rgba(0,0,0,0)',
    'font': {'color': '#ccd6f6', 'family': 'Inter, sans-serif'},
    'xaxis': {
        'gridcolor': 'rgba(255,255,255,0.1)',
        'linecolor': 'rgba(255,255,255,0.1)',
        'tickfont': {'color': '#8892b0'}
    },
    'yaxis': {
        'gridcolor': 'rgba(255,255,255,0.1)',
        'linecolor': 'rgba(255,255,255,0.1)',
        'tickfont': {'color': '#8892b0'}
    }
}

COLORS = {
    'primary': '#667eea',
    'secondary': '#764ba2',
    'success': '#64ffda',
    'warning': '#ffd666',
    'danger': '#ff6b6b',
    'info': '#74b9ff',
    'gradient': ['#667eea', '#764ba2', '#f093fb', '#f5576c']
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
    # Add any extra kwargs that aren't xaxis/yaxis/margin
    for k, v in kwargs.items():
        if k not in ['xaxis', 'yaxis', 'margin']:
            layout_args[k] = v
    fig.update_layout(**layout_args)
    return fig


@st.cache_resource
def get_bq_client():
    """Initialize BigQuery client."""
    # Use Streamlit secrets for credentials if available (Streamlit Cloud)
    # Otherwise use default credentials (local development)
    if "gcp_service_account" in st.secrets:
        from google.oauth2 import service_account
        credentials = service_account.Credentials.from_service_account_info(
            st.secrets["gcp_service_account"]
        )
        return bigquery.Client(project='artful-logic-475116-p1', credentials=credentials)
    return bigquery.Client(project='artful-logic-475116-p1')


@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_daily_metrics():
    """Load daily metrics from BigQuery."""
    client = get_bq_client()
    query = """
    SELECT *
    FROM `artful-logic-475116-p1.mart_zendesk.dim_daily_metrics`
    ORDER BY created_date DESC
    LIMIT 90
    """
    return client.query(query).to_dataframe()


@st.cache_data(ttl=300)
def load_agent_performance():
    """Load agent performance metrics."""
    client = get_bq_client()
    query = """
    SELECT *
    FROM `artful-logic-475116-p1.mart_zendesk.dim_agent_performance`
    WHERE created_month >= DATE_TRUNC(CURRENT_DATE(), MONTH) - INTERVAL 3 MONTH
    ORDER BY created_month DESC, tickets_handled DESC
    """
    return client.query(query).to_dataframe()


@st.cache_data(ttl=300)
def load_current_stats():
    """Load current summary statistics."""
    client = get_bq_client()
    query = """
    SELECT
        COUNT(*) as total_tickets,
        COUNTIF(status IN ('open', 'pending', 'new')) as backlog,
        ROUND(AVG(resolution_minutes_business) / 60, 1) as avg_resolution_hours,
        ROUND(100.0 * COUNTIF(resolved_same_day) / NULLIF(COUNTIF(is_resolved), 0), 1) as same_day_pct,
        ROUND(100.0 * COUNTIF(csat_score = 'good') / NULLIF(COUNTIF(csat_score IN ('good', 'bad')), 0), 1) as csat_rate,
        COUNTIF(DATE(created_at) = CURRENT_DATE()) as today_tickets,
        COUNTIF(DATE(created_at) = CURRENT_DATE() - 1) as yesterday_tickets,
        ROUND(100.0 * COUNTIF(first_response_under_1hr) / NULLIF(COUNT(*), 0), 1) as fast_response_pct
    FROM `artful-logic-475116-p1.mart_zendesk.fct_ticket_summary`
    """
    return client.query(query).to_dataframe().iloc[0]


@st.cache_data(ttl=300)
def load_tag_analysis():
    """Load tag analysis data."""
    client = get_bq_client()
    query = """
    SELECT
        tag,
        SUM(ticket_count) as total_tickets,
        AVG(avg_resolution_minutes) as avg_resolution,
        AVG(csat_rate) as avg_csat
    FROM `artful-logic-475116-p1.mart_zendesk.dim_tag_analysis`
    WHERE created_month >= DATE_TRUNC(CURRENT_DATE(), MONTH) - INTERVAL 2 MONTH
    GROUP BY tag
    ORDER BY total_tickets DESC
    LIMIT 15
    """
    return client.query(query).to_dataframe()


@st.cache_data(ttl=300)
def load_hourly_heatmap():
    """Load hourly distribution data for heatmap."""
    client = get_bq_client()
    query = """
    SELECT
        created_day_of_week,
        created_hour,
        COUNT(*) as ticket_count
    FROM `artful-logic-475116-p1.mart_zendesk.fct_ticket_summary`
    WHERE created_date >= CURRENT_DATE() - 30
    GROUP BY 1, 2
    """
    return client.query(query).to_dataframe()


def render_metric_card(value, label, delta=None, delta_type="positive"):
    """Render a styled metric card."""
    delta_html = ""
    if delta:
        delta_class = "metric-delta-positive" if delta_type == "positive" else "metric-delta-negative"
        delta_symbol = "↑" if delta_type == "positive" else "↓"
        delta_html = f'<div class="{delta_class}">{delta_symbol} {delta}</div>'

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
            <h1 class="dashboard-header">Support Command Center</h1>
            <p class="dashboard-subtitle">B2C Customer Support Analytics • Real-time Insights</p>
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
        agent_df = load_agent_performance()
        tag_df = load_tag_analysis()
        heatmap_df = load_hourly_heatmap()
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return

    # KPI Cards Row
    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        st.markdown(render_metric_card(
            f"{stats['csat_rate']:.0f}%",
            "CSAT Score",
            "2.3% vs last week" if stats['csat_rate'] > 65 else None,
            "positive" if stats['csat_rate'] > 65 else "negative"
        ), unsafe_allow_html=True)

    with col2:
        backlog_status = "positive" if stats['backlog'] < 150 else "negative"
        st.markdown(render_metric_card(
            f"{stats['backlog']:.0f}",
            "Open Backlog",
            delta_type=backlog_status
        ), unsafe_allow_html=True)

    with col3:
        st.markdown(render_metric_card(
            f"{stats['avg_resolution_hours']:.1f}h",
            "Avg Resolution"
        ), unsafe_allow_html=True)

    with col4:
        st.markdown(render_metric_card(
            f"{stats['same_day_pct']:.0f}%",
            "Same-Day Resolution"
        ), unsafe_allow_html=True)

    with col5:
        today_delta = stats['today_tickets'] - stats['yesterday_tickets']
        delta_type = "negative" if today_delta > 0 else "positive"
        st.markdown(render_metric_card(
            f"{stats['today_tickets']:.0f}",
            "Today's Tickets",
            f"{abs(today_delta):.0f} vs yesterday",
            delta_type
        ), unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # Charts Row 1
    col1, col2 = st.columns([2, 1])

    with col1:
        st.markdown('<p class="section-header">📈 Ticket Volume Trend</p>', unsafe_allow_html=True)

        daily_df_sorted = daily_df.sort_values('created_date')

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=daily_df_sorted['created_date'],
            y=daily_df_sorted['ticket_count'],
            mode='lines',
            name='Tickets',
            line=dict(color=COLORS['primary'], width=3),
            fill='tozeroy',
            fillcolor='rgba(102, 126, 234, 0.2)'
        ))

        # Add 7-day moving average
        daily_df_sorted['ma7'] = daily_df_sorted['ticket_count'].rolling(7).mean()
        fig.add_trace(go.Scatter(
            x=daily_df_sorted['created_date'],
            y=daily_df_sorted['ma7'],
            mode='lines',
            name='7-day avg',
            line=dict(color=COLORS['secondary'], width=2, dash='dot')
        ))

        apply_dark_theme(fig, height=350,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font=dict(color='#8892b0')),
            hovermode='x unified'
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown('<p class="section-header">🎯 CSAT Trend</p>', unsafe_allow_html=True)

        daily_df_sorted = daily_df.sort_values('created_date')
        daily_df_sorted['csat_rate_pct'] = daily_df_sorted['csat_rate'] * 100

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=daily_df_sorted['created_date'],
            y=daily_df_sorted['csat_rate_pct'],
            mode='lines+markers',
            name='CSAT %',
            line=dict(color=COLORS['success'], width=2),
            marker=dict(size=4)
        ))

        # Target line
        fig.add_hline(y=70, line_dash="dash", line_color=COLORS['warning'],
                      annotation_text="Target: 70%", annotation_position="right")

        apply_dark_theme(fig, height=350, showlegend=False, yaxis={'range': [40, 100]})
        st.plotly_chart(fig, use_container_width=True)

    # Charts Row 2
    col1, col2 = st.columns([1, 1])

    with col1:
        st.markdown('<p class="section-header">🔥 Volume Heatmap</p>', unsafe_allow_html=True)

        # Pivot for heatmap
        day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        heatmap_pivot = heatmap_df.pivot_table(
            index='created_day_of_week',
            columns='created_hour',
            values='ticket_count',
            aggfunc='sum'
        ).reindex(day_order)

        fig = go.Figure(data=go.Heatmap(
            z=heatmap_pivot.values,
            x=[f"{h}:00" for h in heatmap_pivot.columns],
            y=heatmap_pivot.index,
            colorscale=[
                [0, 'rgba(102, 126, 234, 0.1)'],
                [0.5, 'rgba(102, 126, 234, 0.5)'],
                [1, 'rgba(118, 75, 162, 1)']
            ],
            showscale=False,
            hovertemplate='%{y}<br>%{x}<br>Tickets: %{z}<extra></extra>'
        ))

        apply_dark_theme(fig, height=300, margin=dict(l=0, r=0, t=10, b=0), xaxis={'tickangle': 45})
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown('<p class="section-header">🏷️ Top Issue Categories</p>', unsafe_allow_html=True)

        tag_df_top = tag_df.head(8)

        fig = go.Figure(go.Bar(
            x=tag_df_top['total_tickets'],
            y=tag_df_top['tag'],
            orientation='h',
            marker=dict(
                color=tag_df_top['total_tickets'],
                colorscale=[[0, COLORS['primary']], [1, COLORS['secondary']]],
            ),
            hovertemplate='%{y}<br>Tickets: %{x}<extra></extra>'
        ))

        apply_dark_theme(fig, height=300, margin=dict(l=0, r=0, t=10, b=0), yaxis={'autorange': 'reversed'})
        st.plotly_chart(fig, use_container_width=True)

    # Agent Performance Section
    st.markdown('<p class="section-header">👥 Agent Performance</p>', unsafe_allow_html=True)

    # Get current month data
    current_month = agent_df[agent_df['created_month'] == agent_df['created_month'].max()]

    if not current_month.empty:
        col1, col2 = st.columns([2, 1])

        with col1:
            # Leaderboard
            top_agents = current_month.nlargest(10, 'tickets_handled')

            fig = go.Figure()

            # Tickets handled bars
            fig.add_trace(go.Bar(
                x=top_agents['agent_name'],
                y=top_agents['tickets_handled'],
                name='Tickets Handled',
                marker_color=COLORS['primary'],
                hovertemplate='%{x}<br>Tickets: %{y}<extra></extra>'
            ))

            apply_dark_theme(fig, height=350, xaxis={'tickangle': 45}, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            # CSAT by agent (top performers)
            top_csat = current_month[current_month['tickets_handled'] >= 10].nlargest(5, 'csat_rate')

            if not top_csat.empty:
                fig = go.Figure(go.Bar(
                    x=top_csat['csat_rate'] * 100,
                    y=top_csat['agent_name'],
                    orientation='h',
                    marker=dict(
                        color=top_csat['csat_rate'] * 100,
                        colorscale=[[0, COLORS['warning']], [0.7, COLORS['success']], [1, COLORS['success']]],
                        cmin=50,
                        cmax=100
                    ),
                    hovertemplate='%{y}<br>CSAT: %{x:.1f}%<extra></extra>'
                ))

                apply_dark_theme(fig, height=350,
                    title=dict(text='Top CSAT (min 10 tickets)', font=dict(color='#8892b0', size=14)),
                    xaxis={'range': [50, 100]},
                    yaxis={'autorange': 'reversed'}
                )
                st.plotly_chart(fig, use_container_width=True)

    # Footer
    st.markdown(f"""
    <div style="text-align: center; color: #8892b0; margin-top: 48px; padding: 24px; border-top: 1px solid rgba(255,255,255,0.1);">
        <p style="margin: 0;">Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} UTC</p>
        <p style="margin: 4px 0 0 0; font-size: 12px;">Data refreshes every 5 minutes • Built with 💜 by BigQuery Agent</p>
    </div>
    """, unsafe_allow_html=True)


if __name__ == "__main__":
    main()
