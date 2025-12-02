"""
Visit Performance & Attribution Dashboard
Check-In Attribution • Visit-to-Depletion Conversion • POD Growth Tracking
"""

import streamlit as st
import pandas as pd
from google.cloud import bigquery
from datetime import datetime
import plotly.graph_objects as go

# Page config
st.set_page_config(
    page_title="Visit Performance",
    page_icon="🚀",
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

    .metric-card {
        background: linear-gradient(145deg, #1e1e2f 0%, #2a2a4a 100%);
        border-radius: 16px;
        padding: 24px;
        border: 1px solid rgba(255,255,255,0.1);
        box-shadow: 0 8px 32px rgba(0,0,0,0.3);
        margin-bottom: 16px;
    }
    .metric-card:hover {
        transform: translateY(-4px);
        box-shadow: 0 12px 40px rgba(0,0,0,0.4);
    }
    .metric-value { font-size: 36px; font-weight: 700; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); -webkit-background-clip: text; -webkit-text-fill-color: transparent; margin: 0; }
    .metric-value-green { font-size: 36px; font-weight: 700; background: linear-gradient(135deg, #64ffda 0%, #00bfa5 100%); -webkit-background-clip: text; -webkit-text-fill-color: transparent; margin: 0; }
    .metric-value-gold { font-size: 36px; font-weight: 700; background: linear-gradient(135deg, #ffd700 0%, #ff8c00 100%); -webkit-background-clip: text; -webkit-text-fill-color: transparent; margin: 0; }
    .metric-value-red { font-size: 36px; font-weight: 700; background: linear-gradient(135deg, #ff6b6b 0%, #ee5a24 100%); -webkit-background-clip: text; -webkit-text-fill-color: transparent; margin: 0; }
    .metric-label { font-size: 14px; color: #8892b0; text-transform: uppercase; letter-spacing: 1.5px; margin-top: 8px; }
    .metric-sublabel { font-size: 12px; color: #5a6785; margin-top: 4px; }

    .dashboard-header { background: linear-gradient(90deg, #667eea 0%, #764ba2 100%); -webkit-background-clip: text; -webkit-text-fill-color: transparent; font-size: 42px; font-weight: 800; margin-bottom: 8px; }
    .dashboard-subtitle { color: #8892b0; font-size: 16px; margin-bottom: 32px; }
    .section-header { color: #ccd6f6; font-size: 22px; font-weight: 600; margin: 28px 0 16px 0; padding-bottom: 8px; border-bottom: 2px solid rgba(102, 126, 234, 0.3); }

    .leaderboard-card {
        background: linear-gradient(145deg, #1e1e2f 0%, #252540 100%);
        border-radius: 12px;
        padding: 16px;
        border: 1px solid rgba(255,255,255,0.08);
        margin-bottom: 8px;
    }
    .leaderboard-rank { font-size: 24px; font-weight: 700; color: #667eea; width: 40px; display: inline-block; }
    .leaderboard-rank-gold { color: #ffd700; }
    .leaderboard-rank-silver { color: #c0c0c0; }
    .leaderboard-rank-bronze { color: #cd7f32; }
    .leaderboard-name { color: #ccd6f6; font-weight: 600; font-size: 16px; }
    .leaderboard-stats { color: #8892b0; font-size: 12px; margin-top: 4px; }

    .attribution-badge { background: linear-gradient(135deg, #64ffda 0%, #00bfa5 100%); color: #0f0f1a; padding: 4px 12px; border-radius: 20px; font-size: 11px; font-weight: 700; margin-left: 8px; }
    .pod-badge { background: linear-gradient(135deg, #ffd700 0%, #ff8c00 100%); color: #0f0f1a; padding: 4px 12px; border-radius: 20px; font-size: 11px; font-weight: 700; margin-left: 8px; }

    .live-indicator { display: inline-flex; align-items: center; gap: 8px; color: #64ffda; font-size: 12px; text-transform: uppercase; letter-spacing: 1px; }
    .live-dot { width: 8px; height: 8px; background: #64ffda; border-radius: 50%; animation: pulse 2s infinite; }
    @keyframes pulse { 0%, 100% { opacity: 1; transform: scale(1); } 50% { opacity: 0.5; transform: scale(1.2); } }
</style>
""", unsafe_allow_html=True)

COLORS = {
    'primary': '#667eea',
    'secondary': '#764ba2',
    'success': '#64ffda',
    'warning': '#ffd666',
    'danger': '#ff6b6b',
    'gold': '#ffd700',
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
    if "gcp_service_account" in st.secrets:
        from google.oauth2 import service_account
        credentials = service_account.Credentials.from_service_account_info(
            st.secrets["gcp_service_account"]
        )
        return bigquery.Client(project='artful-logic-475116-p1', credentials=credentials)
    return bigquery.Client(project='artful-logic-475116-p1')


# =============================================================================
# BigQuery Data Loaders
# =============================================================================

@st.cache_data(ttl=300)
def load_visit_summary(days_back=30):
    """Load visit summary metrics."""
    client = get_bq_client()
    query = f"""
    WITH visits AS (
        SELECT
            t.Id as task_id,
            t.AccountId as account_id,
            DATE(t.CompletedDateTime) as visit_date,
            t.OwnerId as rep_id
        FROM `artful-logic-475116-p1.raw_salesforce.Task` t
        WHERE t.Subject LIKE 'Check In%'
          AND t.Status = 'Completed'
          AND t.CompletedDateTime IS NOT NULL
          AND t.AccountId IS NOT NULL
          AND DATE(t.CompletedDateTime) >= DATE_SUB(CURRENT_DATE(), INTERVAL {days_back} DAY)
    )
    SELECT
        COUNT(*) as total_visits,
        COUNT(DISTINCT account_id) as unique_accounts,
        COUNT(DISTINCT rep_id) as active_reps,
        COUNT(DISTINCT visit_date) as active_days
    FROM visits
    """
    return client.query(query).to_dataframe().iloc[0]


@st.cache_data(ttl=300)
def load_visit_attribution(days_back=30, attribution_window=30):
    """Load visit-to-DEPLETION attribution metrics - THE SECRET SAUCE."""
    client = get_bq_client()
    query = f"""
    WITH visits AS (
        SELECT
            t.Id as task_id,
            t.AccountId as account_id,
            DATE(t.CompletedDateTime) as visit_date,
            t.OwnerId as rep_id
        FROM `artful-logic-475116-p1.raw_salesforce.Task` t
        WHERE t.Subject LIKE 'Check In%'
          AND t.Status = 'Completed'
          AND t.CompletedDateTime IS NOT NULL
          AND t.AccountId IS NOT NULL
          AND DATE(t.CompletedDateTime) >= DATE_SUB(CURRENT_DATE(), INTERVAL {days_back + attribution_window} DAY)
          AND DATE(t.CompletedDateTime) <= DATE_SUB(CURRENT_DATE(), INTERVAL {attribution_window} DAY)
    ),
    account_vip_map AS (
        SELECT DISTINCT
            sfdc_account_id,
            vip_account_code
        FROM `artful-logic-475116-p1.staging_vip.retail_customer_fact_sheet_v2`
        WHERE sfdc_account_id IS NOT NULL
    ),
    depletions_post_visit AS (
        SELECT
            v.task_id,
            v.account_id,
            v.visit_date,
            v.rep_id,
            s.transaction_date,
            s.quantity,
            DATE_DIFF(s.transaction_date, v.visit_date, DAY) as days_to_depletion
        FROM visits v
        JOIN account_vip_map m ON v.account_id = m.sfdc_account_id
        JOIN `artful-logic-475116-p1.analytics.vip_sales_clean` s
            ON m.vip_account_code = s.account_code
            AND s.transaction_date > v.visit_date
            AND s.transaction_date <= DATE_ADD(v.visit_date, INTERVAL {attribution_window} DAY)
    )
    SELECT
        COUNT(DISTINCT v.task_id) as total_visits_in_window,
        COUNT(DISTINCT CASE WHEN d.task_id IS NOT NULL THEN v.task_id END) as visits_with_depletions,
        SAFE_DIVIDE(
            COUNT(DISTINCT CASE WHEN d.task_id IS NOT NULL THEN v.task_id END),
            COUNT(DISTINCT v.task_id)
        ) * 100 as conversion_rate,
        COALESCE(SUM(d.quantity), 0) as total_units_attributed,
        COALESCE(AVG(d.days_to_depletion), 0) as avg_days_to_depletion,
        COUNT(DISTINCT d.account_id) as accounts_with_depletions
    FROM visits v
    LEFT JOIN depletions_post_visit d ON v.task_id = d.task_id
    """
    return client.query(query).to_dataframe().iloc[0]


@st.cache_data(ttl=300)
def load_rep_performance(days_back=30, attribution_window=30):
    """Load rep-level performance with depletion attribution."""
    client = get_bq_client()
    query = f"""
    WITH visits AS (
        SELECT
            t.Id as task_id,
            t.AccountId as account_id,
            DATE(t.CompletedDateTime) as visit_date,
            t.OwnerId as rep_id
        FROM `artful-logic-475116-p1.raw_salesforce.Task` t
        WHERE t.Subject LIKE 'Check In%'
          AND t.Status = 'Completed'
          AND t.CompletedDateTime IS NOT NULL
          AND t.AccountId IS NOT NULL
          AND DATE(t.CompletedDateTime) >= DATE_SUB(CURRENT_DATE(), INTERVAL {days_back} DAY)
    ),
    rep_names AS (
        SELECT Id, Name
        FROM `artful-logic-475116-p1.raw_salesforce.User`
    ),
    account_vip_map AS (
        SELECT DISTINCT
            sfdc_account_id,
            vip_account_code
        FROM `artful-logic-475116-p1.staging_vip.retail_customer_fact_sheet_v2`
        WHERE sfdc_account_id IS NOT NULL
    ),
    depletions_post_visit AS (
        SELECT
            v.task_id,
            v.rep_id,
            s.quantity,
            DATE_DIFF(s.transaction_date, v.visit_date, DAY) as days_to_depletion
        FROM visits v
        JOIN account_vip_map m ON v.account_id = m.sfdc_account_id
        JOIN `artful-logic-475116-p1.analytics.vip_sales_clean` s
            ON m.vip_account_code = s.account_code
            AND s.transaction_date > v.visit_date
            AND s.transaction_date <= DATE_ADD(v.visit_date, INTERVAL {attribution_window} DAY)
    ),
    rep_stats AS (
        SELECT
            v.rep_id,
            COUNT(DISTINCT v.task_id) as total_visits,
            COUNT(DISTINCT v.account_id) as unique_accounts,
            COUNT(DISTINCT CASE WHEN d.task_id IS NOT NULL THEN v.task_id END) as visits_with_depletions,
            COALESCE(SUM(d.quantity), 0) as units_attributed,
            COALESCE(AVG(d.days_to_depletion), 0) as avg_days_to_depletion
        FROM visits v
        LEFT JOIN depletions_post_visit d ON v.task_id = d.task_id
        GROUP BY v.rep_id
    )
    SELECT
        r.Name as rep_name,
        s.total_visits,
        s.unique_accounts,
        s.visits_with_depletions,
        SAFE_DIVIDE(s.visits_with_depletions, s.total_visits) * 100 as conversion_rate,
        s.units_attributed,
        s.avg_days_to_depletion
    FROM rep_stats s
    JOIN rep_names r ON s.rep_id = r.Id
    WHERE s.total_visits >= 5
    ORDER BY s.units_attributed DESC
    """
    return client.query(query).to_dataframe()


@st.cache_data(ttl=300)
def load_pod_growth(days_back=60, attribution_window=30):
    """Load POD growth after visits."""
    client = get_bq_client()
    query = f"""
    WITH visits AS (
        SELECT
            t.Id as task_id,
            t.AccountId as account_id,
            DATE(t.CompletedDateTime) as visit_date,
            t.OwnerId as rep_id
        FROM `artful-logic-475116-p1.raw_salesforce.Task` t
        WHERE t.Subject LIKE 'Check In%'
          AND t.Status = 'Completed'
          AND t.CompletedDateTime IS NOT NULL
          AND t.AccountId IS NOT NULL
          AND DATE(t.CompletedDateTime) >= DATE_SUB(CURRENT_DATE(), INTERVAL {days_back + attribution_window} DAY)
          AND DATE(t.CompletedDateTime) <= DATE_SUB(CURRENT_DATE(), INTERVAL {attribution_window} DAY)
    ),
    account_vip_map AS (
        SELECT DISTINCT
            sfdc_account_id,
            vip_account_code
        FROM `artful-logic-475116-p1.staging_vip.retail_customer_fact_sheet_v2`
        WHERE sfdc_account_id IS NOT NULL
    ),
    pods_before AS (
        SELECT
            v.task_id,
            v.account_id,
            COUNT(DISTINCT s.product_code) as pod_count_before
        FROM visits v
        JOIN account_vip_map m ON v.account_id = m.sfdc_account_id
        JOIN `artful-logic-475116-p1.analytics.vip_sales_clean` s
            ON m.vip_account_code = s.account_code
            AND s.transaction_date >= DATE_SUB(v.visit_date, INTERVAL 30 DAY)
            AND s.transaction_date < v.visit_date
        GROUP BY v.task_id, v.account_id
    ),
    pods_after AS (
        SELECT
            v.task_id,
            v.account_id,
            COUNT(DISTINCT s.product_code) as pod_count_after
        FROM visits v
        JOIN account_vip_map m ON v.account_id = m.sfdc_account_id
        JOIN `artful-logic-475116-p1.analytics.vip_sales_clean` s
            ON m.vip_account_code = s.account_code
            AND s.transaction_date > v.visit_date
            AND s.transaction_date <= DATE_ADD(v.visit_date, INTERVAL {attribution_window} DAY)
        GROUP BY v.task_id, v.account_id
    )
    SELECT
        COUNT(DISTINCT v.task_id) as visits_with_pod_data,
        AVG(COALESCE(b.pod_count_before, 0)) as avg_pods_before,
        AVG(COALESCE(a.pod_count_after, 0)) as avg_pods_after,
        SUM(CASE WHEN COALESCE(a.pod_count_after, 0) > COALESCE(b.pod_count_before, 0) THEN 1 ELSE 0 END) as visits_with_pod_increase,
        SAFE_DIVIDE(
            SUM(CASE WHEN COALESCE(a.pod_count_after, 0) > COALESCE(b.pod_count_before, 0) THEN 1 ELSE 0 END),
            COUNT(DISTINCT v.task_id)
        ) * 100 as pod_increase_rate
    FROM visits v
    LEFT JOIN pods_before b ON v.task_id = b.task_id
    LEFT JOIN pods_after a ON v.task_id = a.task_id
    WHERE b.pod_count_before IS NOT NULL OR a.pod_count_after IS NOT NULL
    """
    return client.query(query).to_dataframe().iloc[0]


@st.cache_data(ttl=300)
def load_weekly_trend(weeks_back=12):
    """Load weekly visit and depletion attribution trend."""
    client = get_bq_client()
    query = f"""
    WITH visits AS (
        SELECT
            t.Id as task_id,
            t.AccountId as account_id,
            DATE(t.CompletedDateTime) as visit_date,
            DATE_TRUNC(DATE(t.CompletedDateTime), WEEK) as visit_week
        FROM `artful-logic-475116-p1.raw_salesforce.Task` t
        WHERE t.Subject LIKE 'Check In%'
          AND t.Status = 'Completed'
          AND t.CompletedDateTime IS NOT NULL
          AND t.AccountId IS NOT NULL
          AND DATE(t.CompletedDateTime) >= DATE_SUB(CURRENT_DATE(), INTERVAL {weeks_back * 7 + 30} DAY)
    ),
    account_vip_map AS (
        SELECT DISTINCT
            sfdc_account_id,
            vip_account_code
        FROM `artful-logic-475116-p1.staging_vip.retail_customer_fact_sheet_v2`
        WHERE sfdc_account_id IS NOT NULL
    ),
    depletions_post_visit AS (
        SELECT DISTINCT
            v.task_id,
            v.visit_week
        FROM visits v
        JOIN account_vip_map m ON v.account_id = m.sfdc_account_id
        JOIN `artful-logic-475116-p1.analytics.vip_sales_clean` s
            ON m.vip_account_code = s.account_code
            AND s.transaction_date > v.visit_date
            AND s.transaction_date <= DATE_ADD(v.visit_date, INTERVAL 30 DAY)
    )
    SELECT
        v.visit_week,
        COUNT(DISTINCT v.task_id) as total_visits,
        COUNT(DISTINCT CASE WHEN d.task_id IS NOT NULL THEN v.task_id END) as visits_converted,
        SAFE_DIVIDE(
            COUNT(DISTINCT CASE WHEN d.task_id IS NOT NULL THEN v.task_id END),
            COUNT(DISTINCT v.task_id)
        ) * 100 as conversion_rate
    FROM visits v
    LEFT JOIN depletions_post_visit d ON v.task_id = d.task_id
    WHERE v.visit_week <= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    GROUP BY v.visit_week
    ORDER BY v.visit_week
    """
    return client.query(query).to_dataframe()


@st.cache_data(ttl=300)
def load_this_week_visits():
    """Load this week's visits."""
    client = get_bq_client()
    query = """
    SELECT COUNT(*) as visits_this_week
    FROM `artful-logic-475116-p1.raw_salesforce.Task` t
    WHERE t.Subject LIKE 'Check In%'
      AND t.Status = 'Completed'
      AND t.CompletedDateTime IS NOT NULL
      AND DATE(t.CompletedDateTime) >= DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY))
    """
    return client.query(query).to_dataframe().iloc[0]['visits_this_week']


# =============================================================================
# UI Components
# =============================================================================

def render_metric_card(value, label, sublabel=None, status="neutral"):
    value_class = {
        "healthy": "metric-value-green",
        "gold": "metric-value-gold",
        "critical": "metric-value-red",
        "neutral": "metric-value"
    }.get(status, "metric-value")
    sublabel_html = f'<div class="metric-sublabel">{sublabel}</div>' if sublabel else ""
    return f"""
    <div class="metric-card">
        <div class="{value_class}">{value}</div>
        <div class="metric-label">{label}</div>
        {sublabel_html}
    </div>
    """


def render_leaderboard_entry(rank, name, visits, conversion_rate, units, show_badges=True):
    import html
    rank_class = {1: "leaderboard-rank-gold", 2: "leaderboard-rank-silver", 3: "leaderboard-rank-bronze"}.get(rank, "")
    safe_name = html.escape(str(name)) if name else "Unknown"
    badges_html = ""
    if show_badges:
        if conversion_rate >= 50:
            badges_html += '<span class="attribution-badge">HIGH CONVERTER</span>'
        if units >= 500:
            badges_html += '<span class="pod-badge">TOP SELLER</span>'
    stats_text = f"{int(visits)} visits &bull; {conversion_rate:.0f}% converted &bull; {units:,.0f} units attributed"
    return f'<div class="leaderboard-card"><span class="leaderboard-rank {rank_class}">#{rank}</span> <span class="leaderboard-name">{safe_name}</span>{badges_html}<div class="leaderboard-stats">{stats_text}</div></div>'


# =============================================================================
# Main Dashboard
# =============================================================================

def main():
    # Sidebar filters
    with st.sidebar:
        st.markdown("### Filters")
        days_back = st.selectbox(
            "Time Period",
            options=[7, 14, 30, 60, 90],
            index=2,
            format_func=lambda x: f"Last {x} days"
        )
        attribution_window = st.selectbox(
            "Attribution Window",
            options=[7, 14, 30],
            index=2,
            format_func=lambda x: f"{x} days post-visit"
        )
        st.markdown("---")
        st.markdown("""
        **Attribution Logic**

        Visits are credited with depletions (sell-through) at the same account within the attribution window.

        **POD Growth** tracks SKU expansion after visits.
        """)

    # Header
    st.markdown("""
    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 32px;">
        <div>
            <h1 class="dashboard-header">Visit Performance</h1>
            <p class="dashboard-subtitle">Check-In Attribution • Visit-to-Depletion Conversion • POD Growth</p>
        </div>
        <div class="live-indicator">
            <span class="live-dot"></span>
            Live Data
        </div>
    </div>
    """, unsafe_allow_html=True)

    # Load data
    try:
        visit_summary = load_visit_summary(days_back)
        attribution = load_visit_attribution(days_back, attribution_window)
        rep_performance = load_rep_performance(days_back, attribution_window)
        pod_growth = load_pod_growth(days_back, attribution_window)
        weekly_trend = load_weekly_trend(12)
        visits_this_week = load_this_week_visits()
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return

    # Row 1: Key Metrics
    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        st.markdown(render_metric_card(
            f"{visits_this_week:,}",
            "Visits This Week",
            "Current activity",
            "neutral"
        ), unsafe_allow_html=True)

    with col2:
        st.markdown(render_metric_card(
            f"{visit_summary['total_visits']:,}",
            f"Visits ({days_back}d)",
            f"{visit_summary['unique_accounts']:,} accounts",
            "neutral"
        ), unsafe_allow_html=True)

    with col3:
        conv_rate = attribution['conversion_rate']
        conv_status = "healthy" if conv_rate >= 30 else "gold" if conv_rate >= 15 else "critical"
        st.markdown(render_metric_card(
            f"{conv_rate:.1f}%",
            "Conversion Rate",
            f"{attribution['visits_with_depletions']:,.0f} visits → depletions",
            conv_status
        ), unsafe_allow_html=True)

    with col4:
        st.markdown(render_metric_card(
            f"{attribution['total_units_attributed']:,.0f}",
            "Units Attributed",
            f"Avg {attribution['avg_days_to_depletion']:.1f} days to depletion",
            "gold"
        ), unsafe_allow_html=True)

    with col5:
        pod_rate = pod_growth['pod_increase_rate'] if pd.notna(pod_growth['pod_increase_rate']) else 0
        pod_status = "healthy" if pod_rate >= 20 else "gold" if pod_rate >= 10 else "neutral"
        st.markdown(render_metric_card(
            f"{pod_rate:.1f}%",
            "POD Increase Rate",
            "Visits with SKU expansion",
            pod_status
        ), unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # Row 2: Leaderboard & Trend
    col1, col2 = st.columns([1, 2])

    with col1:
        st.markdown('<p class="section-header">Rep Leaderboard (by Depletions)</p>', unsafe_allow_html=True)
        if not rep_performance.empty:
            for rank, (idx, row) in enumerate(rep_performance.head(10).iterrows(), start=1):
                st.markdown(render_leaderboard_entry(
                    rank=rank,
                    name=row['rep_name'],
                    visits=row['total_visits'],
                    conversion_rate=row['conversion_rate'] if pd.notna(row['conversion_rate']) else 0,
                    units=row['units_attributed']
                ), unsafe_allow_html=True)
        else:
            st.info("No rep data available")

    with col2:
        st.markdown('<p class="section-header">Weekly Depletion Attribution Trend</p>', unsafe_allow_html=True)
        if not weekly_trend.empty:
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=weekly_trend['visit_week'],
                y=weekly_trend['total_visits'],
                name='Total Visits',
                marker_color=COLORS['primary'],
                opacity=0.7
            ))
            fig.add_trace(go.Bar(
                x=weekly_trend['visit_week'],
                y=weekly_trend['visits_converted'],
                name='Visits w/ Depletions',
                marker_color=COLORS['success'],
                opacity=0.9
            ))
            fig.add_trace(go.Scatter(
                x=weekly_trend['visit_week'],
                y=weekly_trend['conversion_rate'],
                name='Conversion Rate %',
                yaxis='y2',
                line=dict(color=COLORS['gold'], width=3),
                mode='lines+markers'
            ))
            apply_dark_theme(fig, height=400,
                barmode='overlay',
                yaxis2=dict(
                    title='Conversion Rate %',
                    overlaying='y',
                    side='right',
                    gridcolor='rgba(255,255,255,0.05)',
                    tickfont={'color': COLORS['gold']},
                    range=[0, 100]
                ),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5)
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No trend data available")

    # Row 3: Rep Table
    st.markdown('<p class="section-header">Rep Performance Details</p>', unsafe_allow_html=True)
    if not rep_performance.empty:
        display_df = rep_performance.copy()
        display_df.columns = ['Rep', 'Visits', 'Accounts', 'w/ Depletions', 'Conv %', 'Units', 'Avg Days']
        display_df['Conv %'] = display_df['Conv %'].apply(lambda x: f"{x:.1f}%" if pd.notna(x) else "0%")
        display_df['Units'] = display_df['Units'].apply(lambda x: f"{x:,.0f}")
        display_df['Avg Days'] = display_df['Avg Days'].apply(lambda x: f"{x:.1f}" if pd.notna(x) else "-")
        st.dataframe(display_df, use_container_width=True, hide_index=True)

    # Footer
    st.markdown(f"""
    <div style="text-align: center; color: #8892b0; margin-top: 48px; padding: 24px; border-top: 1px solid rgba(255,255,255,0.1);">
        <p style="margin: 0;">Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} UTC</p>
        <p style="margin: 4px 0 0 0; font-size: 12px;">Attribution window: {attribution_window} days • Data refreshes every 5 minutes</p>
    </div>
    """, unsafe_allow_html=True)


if __name__ == "__main__":
    main()
