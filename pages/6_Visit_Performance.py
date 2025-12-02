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

# Dark mode custom CSS with responsive design
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
    .metric-value { font-size: clamp(1.5rem, 4vw, 2.25rem); font-weight: 700; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); -webkit-background-clip: text; -webkit-text-fill-color: transparent; margin: 0; }
    .metric-value-green { font-size: clamp(1.5rem, 4vw, 2.25rem); font-weight: 700; background: linear-gradient(135deg, #64ffda 0%, #00bfa5 100%); -webkit-background-clip: text; -webkit-text-fill-color: transparent; margin: 0; }
    .metric-value-gold { font-size: clamp(1.5rem, 4vw, 2.25rem); font-weight: 700; background: linear-gradient(135deg, #ffd666 0%, #f39c12 100%); -webkit-background-clip: text; -webkit-text-fill-color: transparent; margin: 0; }
    .metric-value-red { font-size: clamp(1.5rem, 4vw, 2.25rem); font-weight: 700; background: linear-gradient(135deg, #ff6b6b 0%, #ee5a24 100%); -webkit-background-clip: text; -webkit-text-fill-color: transparent; margin: 0; }
    .metric-label { font-size: clamp(0.7rem, 1.5vw, 0.875rem); color: #8892b0; text-transform: uppercase; letter-spacing: 1px; margin-top: 6px; }
    .metric-sublabel { font-size: clamp(0.65rem, 1.2vw, 0.75rem); color: #5a6785; margin-top: 4px; }

    .dashboard-header { background: linear-gradient(90deg, #667eea 0%, #764ba2 100%); -webkit-background-clip: text; -webkit-text-fill-color: transparent; font-size: clamp(1.75rem, 5vw, 2.625rem); font-weight: 800; margin-bottom: 8px; }
    .dashboard-subtitle { color: #8892b0; font-size: clamp(0.875rem, 2vw, 1rem); margin-bottom: 24px; }
    .section-header { color: #ccd6f6; font-size: clamp(1.1rem, 2.5vw, 1.375rem); font-weight: 600; margin: 24px 0 12px 0; padding-bottom: 8px; border-bottom: 2px solid rgba(102, 126, 234, 0.3); }

    .leaderboard-card {
        background: linear-gradient(145deg, #1e1e2f 0%, #2a2a4a 100%);
        border-radius: 12px;
        padding: clamp(12px, 2vw, 16px);
        border: 1px solid rgba(255,255,255,0.08);
        margin-bottom: 8px;
    }
    .leaderboard-rank { font-size: clamp(1.25rem, 3vw, 1.5rem); font-weight: 700; color: #ccd6f6; width: 40px; display: inline-block; }
    .leaderboard-rank-gold { color: #ffd666; }
    .leaderboard-rank-silver { color: #c0c0c0; }
    .leaderboard-rank-bronze { color: #cd7f32; }
    .leaderboard-name { color: #ccd6f6; font-weight: 600; font-size: clamp(0.9rem, 2vw, 1rem); }
    .leaderboard-stats { color: #8892b0; font-size: clamp(0.7rem, 1.5vw, 0.75rem); margin-top: 4px; }

    /* BAN metrics in leaderboard cards */
    .leaderboard-card .ban-metrics {
        display: flex;
        gap: clamp(8px, 2vw, 16px);
        margin-top: 8px;
        flex-wrap: wrap;
    }
    .leaderboard-card .ban-metric {
        text-align: center;
        min-width: 50px;
    }
    .leaderboard-card .ban-value {
        font-size: clamp(1rem, 2vw, 1.25rem);
        font-weight: 700;
    }
    .leaderboard-card .ban-label {
        font-size: clamp(0.55rem, 1vw, 0.65rem);
        color: #8892b0;
        text-transform: uppercase;
    }

    .attribution-badge { background: linear-gradient(135deg, #64ffda 0%, #00bfa5 100%); color: #0f0f1a; padding: 3px 10px; border-radius: 20px; font-size: clamp(0.6rem, 1.2vw, 0.7rem); font-weight: 700; margin-left: 8px; }
    .pod-badge { background: linear-gradient(135deg, #ffd666 0%, #f39c12 100%); color: #0f0f1a; padding: 3px 10px; border-radius: 20px; font-size: clamp(0.6rem, 1.2vw, 0.7rem); font-weight: 700; margin-left: 8px; }

    .live-indicator { display: inline-flex; align-items: center; gap: 8px; color: #64ffda; font-size: clamp(0.65rem, 1.5vw, 0.75rem); text-transform: uppercase; letter-spacing: 1px; }
    .live-dot { width: 8px; height: 8px; background: #64ffda; border-radius: 50%; animation: pulse 2s infinite; }
    @keyframes pulse { 0%, 100% { opacity: 1; transform: scale(1); } 50% { opacity: 0.5; transform: scale(1.2); } }

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
        .leaderboard-card .ban-metrics {
            gap: 12px;
        }
    }
</style>
""", unsafe_allow_html=True)

COLORS = {'primary': '#667eea', 'secondary': '#764ba2', 'success': '#64ffda', 'warning': '#ffd666', 'danger': '#ff6b6b', 'info': '#74b9ff'}


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


# Hardcoded attribution start date for bonus period
ATTRIBUTION_START_DATE = '2025-11-17'
# Cache version - increment to force cache refresh
CACHE_VERSION = 3


@st.cache_data(ttl=300)
def load_bonus_period_visits(_cache_version=CACHE_VERSION):
    """Load ALL visits since bonus period start (effort metric - real-time)."""
    client = get_bq_client()
    query = f"""
    SELECT
        COUNT(*) as total_visits,
        COUNT(DISTINCT AccountId) as unique_accounts,
        COUNT(DISTINCT OwnerId) as active_reps,
        COUNT(DISTINCT DATE(CompletedDateTime)) as active_days,
        -- Visits that can be measured (14+ days old)
        COUNTIF(DATE(CompletedDateTime) <= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)) as measurable_visits,
        -- Visits still pending attribution window
        COUNTIF(DATE(CompletedDateTime) > DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)) as pending_visits
    FROM `artful-logic-475116-p1.raw_salesforce.Task`
    WHERE Subject LIKE 'Check In%'
      AND Status = 'Completed'
      AND CompletedDateTime IS NOT NULL
      AND AccountId IS NOT NULL
      AND DATE(CompletedDateTime) >= '{ATTRIBUTION_START_DATE}'
    """
    return client.query(query).to_dataframe().iloc[0]


@st.cache_data(ttl=300)
def load_visit_attribution(days_back=30, attribution_window=30, rep_name=None, _cache_version=CACHE_VERSION):
    """Load visit attribution metrics - before/after comparison model.

    Conversion = visit led to GROWTH (new PODs or increased volume)
    Attribution = only the incremental units, not total volume
    """
    client = get_bq_client()
    rep_filter = ""
    if rep_name and rep_name != "All Reps":
        rep_filter = f"AND u.Name = '{rep_name}'"
    query = f"""
    WITH visits AS (
        SELECT
            t.Id as task_id,
            t.AccountId as account_id,
            DATE(t.CompletedDateTime) as visit_date,
            t.OwnerId as rep_id
        FROM `artful-logic-475116-p1.raw_salesforce.Task` t
        JOIN `artful-logic-475116-p1.raw_salesforce.User` u ON t.OwnerId = u.Id
        WHERE t.Subject LIKE 'Check In%'
          AND t.Status = 'Completed'
          AND t.CompletedDateTime IS NOT NULL
          AND t.AccountId IS NOT NULL
          AND DATE(t.CompletedDateTime) >= '{ATTRIBUTION_START_DATE}'
          AND DATE(t.CompletedDateTime) <= DATE_SUB(CURRENT_DATE(), INTERVAL {attribution_window} DAY)
          {rep_filter}
    ),
    account_vip_map AS (
        SELECT DISTINCT
            sfdc_account_id,
            vip_id,
            vip_account_code,
            distributor_code
        FROM `artful-logic-475116-p1.staging_vip.retail_customer_fact_sheet_v2`
        WHERE sfdc_account_id IS NOT NULL
    ),
    -- Volume by product BEFORE visit (30d lookback)
    volume_before AS (
        SELECT
            v.task_id,
            s.product_code,
            SUM(s.quantity) as units_before
        FROM visits v
        JOIN account_vip_map m ON v.account_id = m.sfdc_account_id
        JOIN `artful-logic-475116-p1.analytics.vip_sales_clean` s
            ON m.vip_account_code = s.account_code
            AND m.distributor_code = s.distributor_code
            AND s.transaction_date >= DATE_SUB(v.visit_date, INTERVAL 30 DAY)
            AND s.transaction_date < v.visit_date
        GROUP BY v.task_id, s.product_code
    ),
    -- Volume by product AFTER visit (attribution window)
    volume_after AS (
        SELECT
            v.task_id,
            s.product_code,
            SUM(s.quantity) as units_after
        FROM visits v
        JOIN account_vip_map m ON v.account_id = m.sfdc_account_id
        JOIN `artful-logic-475116-p1.analytics.vip_sales_clean` s
            ON m.vip_account_code = s.account_code
            AND m.distributor_code = s.distributor_code
            AND s.transaction_date > v.visit_date
            AND s.transaction_date <= DATE_ADD(v.visit_date, INTERVAL {attribution_window} DAY)
        GROUP BY v.task_id, s.product_code
    ),
    -- Calculate attribution per visit
    visit_attribution AS (
        SELECT
            COALESCE(a.task_id, b.task_id) as task_id,
            COALESCE(a.product_code, b.product_code) as product_code,
            COALESCE(b.units_before, 0) as units_before,
            COALESCE(a.units_after, 0) as units_after,
            -- New POD = product bought after but not before
            CASE WHEN b.units_before IS NULL AND a.units_after > 0 THEN a.units_after ELSE 0 END as new_pod_units,
            -- Incremental = growth in existing products (only positive delta)
            CASE WHEN b.units_before IS NOT NULL AND a.units_after > b.units_before
                 THEN a.units_after - b.units_before ELSE 0 END as incremental_units
        FROM volume_after a
        FULL OUTER JOIN volume_before b
            ON a.task_id = b.task_id AND a.product_code = b.product_code
        WHERE a.units_after IS NOT NULL  -- Only care about post-visit activity
    ),
    -- Aggregate to visit level
    visit_totals AS (
        SELECT
            task_id,
            SUM(new_pod_units) as new_pod_units,
            SUM(incremental_units) as incremental_units,
            SUM(new_pod_units + incremental_units) as total_attributed_units,
            COUNT(DISTINCT CASE WHEN new_pod_units > 0 THEN product_code END) as new_pods_count
        FROM visit_attribution
        GROUP BY task_id
    )
    SELECT
        COUNT(DISTINCT v.task_id) as measurable_visits,
        COUNT(DISTINCT CASE WHEN vt.total_attributed_units > 0 THEN v.task_id END) as visits_converted,
        SAFE_DIVIDE(
            COUNT(DISTINCT CASE WHEN vt.total_attributed_units > 0 THEN v.task_id END),
            COUNT(DISTINCT v.task_id)
        ) * 100 as conversion_rate,
        COALESCE(SUM(vt.new_pod_units), 0) as new_pod_units,
        COALESCE(SUM(vt.incremental_units), 0) as incremental_units,
        COALESCE(SUM(vt.total_attributed_units), 0) as total_attributed_units,
        COALESCE(SUM(vt.new_pods_count), 0) as total_new_pods
    FROM visits v
    LEFT JOIN visit_totals vt ON v.task_id = vt.task_id
    """
    return client.query(query).to_dataframe().iloc[0]


@st.cache_data(ttl=300)
def load_rep_performance(days_back=30, attribution_window=30, _cache_version=CACHE_VERSION):
    """Load rep-level performance with before/after attribution model."""
    client = get_bq_client()
    query = f"""
    WITH all_visits AS (
        -- All visits since Nov 17 for effort count
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
          AND DATE(t.CompletedDateTime) >= '{ATTRIBUTION_START_DATE}'
    ),
    visits AS (
        -- Only visits old enough for attribution calculation
        SELECT * FROM all_visits
        WHERE visit_date <= DATE_SUB(CURRENT_DATE(), INTERVAL {attribution_window} DAY)
    ),
    rep_names AS (
        SELECT Id, Name
        FROM `artful-logic-475116-p1.raw_salesforce.User`
    ),
    account_vip_map AS (
        SELECT DISTINCT
            sfdc_account_id,
            vip_id,
            vip_account_code,
            distributor_code
        FROM `artful-logic-475116-p1.staging_vip.retail_customer_fact_sheet_v2`
        WHERE sfdc_account_id IS NOT NULL
    ),
    -- Volume by product BEFORE visit (30d lookback)
    volume_before AS (
        SELECT
            v.task_id,
            v.rep_id,
            s.product_code,
            SUM(s.quantity) as units_before
        FROM visits v
        JOIN account_vip_map m ON v.account_id = m.sfdc_account_id
        JOIN `artful-logic-475116-p1.analytics.vip_sales_clean` s
            ON m.vip_account_code = s.account_code
            AND m.distributor_code = s.distributor_code
            AND s.transaction_date >= DATE_SUB(v.visit_date, INTERVAL 30 DAY)
            AND s.transaction_date < v.visit_date
        GROUP BY v.task_id, v.rep_id, s.product_code
    ),
    -- Volume by product AFTER visit (attribution window)
    volume_after AS (
        SELECT
            v.task_id,
            v.rep_id,
            s.product_code,
            SUM(s.quantity) as units_after
        FROM visits v
        JOIN account_vip_map m ON v.account_id = m.sfdc_account_id
        JOIN `artful-logic-475116-p1.analytics.vip_sales_clean` s
            ON m.vip_account_code = s.account_code
            AND m.distributor_code = s.distributor_code
            AND s.transaction_date > v.visit_date
            AND s.transaction_date <= DATE_ADD(v.visit_date, INTERVAL {attribution_window} DAY)
        GROUP BY v.task_id, v.rep_id, s.product_code
    ),
    -- Calculate attribution per visit/product
    visit_attribution AS (
        SELECT
            COALESCE(a.task_id, b.task_id) as task_id,
            COALESCE(a.rep_id, b.rep_id) as rep_id,
            COALESCE(a.product_code, b.product_code) as product_code,
            COALESCE(b.units_before, 0) as units_before,
            COALESCE(a.units_after, 0) as units_after,
            CASE WHEN b.units_before IS NULL AND a.units_after > 0 THEN a.units_after ELSE 0 END as new_pod_units,
            CASE WHEN b.units_before IS NOT NULL AND a.units_after > b.units_before
                 THEN a.units_after - b.units_before ELSE 0 END as incremental_units
        FROM volume_after a
        FULL OUTER JOIN volume_before b
            ON a.task_id = b.task_id AND a.product_code = b.product_code
        WHERE a.units_after IS NOT NULL
    ),
    -- Aggregate to visit level
    visit_totals AS (
        SELECT
            task_id,
            rep_id,
            SUM(new_pod_units) as new_pod_units,
            SUM(incremental_units) as incremental_units,
            SUM(new_pod_units + incremental_units) as total_attributed_units,
            COUNT(DISTINCT CASE WHEN new_pod_units > 0 THEN product_code END) as new_pods_count
        FROM visit_attribution
        GROUP BY task_id, rep_id
    ),
    -- All visits count per rep (for effort)
    all_visit_counts AS (
        SELECT
            rep_id,
            COUNT(DISTINCT task_id) as total_visits,
            COUNT(DISTINCT account_id) as unique_accounts
        FROM all_visits
        GROUP BY rep_id
    ),
    -- Attribution stats (only from visits old enough)
    attribution_stats AS (
        SELECT
            v.rep_id,
            COUNT(DISTINCT v.task_id) as measurable_visits,
            COUNT(DISTINCT CASE WHEN vt.total_attributed_units > 0 THEN v.task_id END) as visits_converted,
            COALESCE(SUM(vt.new_pod_units), 0) as new_pod_units,
            COALESCE(SUM(vt.incremental_units), 0) as incremental_units,
            COALESCE(SUM(vt.total_attributed_units), 0) as total_attributed_units,
            COALESCE(SUM(vt.new_pods_count), 0) as new_pods_count
        FROM visits v
        LEFT JOIN visit_totals vt ON v.task_id = vt.task_id
        GROUP BY v.rep_id
    )
    SELECT
        r.Name as rep_name,
        avc.total_visits,
        avc.unique_accounts,
        COALESCE(att.measurable_visits, 0) as measurable_visits,
        COALESCE(att.visits_converted, 0) as visits_converted,
        SAFE_DIVIDE(COALESCE(att.visits_converted, 0), COALESCE(att.measurable_visits, 1)) * 100 as conversion_rate,
        COALESCE(att.new_pod_units, 0) as new_pod_units,
        COALESCE(att.incremental_units, 0) as incremental_units,
        COALESCE(att.total_attributed_units, 0) as total_attributed_units,
        COALESCE(att.new_pods_count, 0) as new_pods_count
    FROM all_visit_counts avc
    JOIN rep_names r ON avc.rep_id = r.Id
    LEFT JOIN attribution_stats att ON avc.rep_id = att.rep_id
    WHERE avc.total_visits >= 5
    ORDER BY avc.total_visits DESC
    """
    return client.query(query).to_dataframe()


@st.cache_data(ttl=300)
def load_rep_weekly_visits(_cache_version=CACHE_VERSION):
    """Load weekly visit counts per rep for sparkline charts."""
    client = get_bq_client()
    query = f"""
    SELECT
        u.Name as rep_name,
        DATE_TRUNC(DATE(t.CompletedDateTime), WEEK(MONDAY)) as week_start,
        COUNT(*) as visits
    FROM `artful-logic-475116-p1.raw_salesforce.Task` t
    JOIN `artful-logic-475116-p1.raw_salesforce.User` u ON t.OwnerId = u.Id
    WHERE t.Subject LIKE 'Check In%'
      AND t.Status = 'Completed'
      AND t.CompletedDateTime IS NOT NULL
      AND DATE(t.CompletedDateTime) >= '{ATTRIBUTION_START_DATE}'
    GROUP BY u.Name, DATE_TRUNC(DATE(t.CompletedDateTime), WEEK(MONDAY))
    ORDER BY u.Name, week_start
    """
    return client.query(query).to_dataframe()


@st.cache_data(ttl=300)
def load_pod_growth(days_back=60, attribution_window=30, _cache_version=CACHE_VERSION):
    """Load POD growth metrics - counts NEW SKUs added after visits."""
    client = get_bq_client()
    query = f"""
    WITH visits AS (
        SELECT
            t.Id as task_id,
            t.AccountId as account_id,
            DATE(t.CompletedDateTime) as visit_date
        FROM `artful-logic-475116-p1.raw_salesforce.Task` t
        WHERE t.Subject LIKE 'Check In%'
          AND t.Status = 'Completed'
          AND t.CompletedDateTime IS NOT NULL
          AND t.AccountId IS NOT NULL
          AND DATE(t.CompletedDateTime) >= '{ATTRIBUTION_START_DATE}'
          AND DATE(t.CompletedDateTime) <= DATE_SUB(CURRENT_DATE(), INTERVAL {attribution_window} DAY)
    ),
    account_vip_map AS (
        SELECT DISTINCT
            sfdc_account_id,
            vip_id,
            vip_account_code,
            distributor_code
        FROM `artful-logic-475116-p1.staging_vip.retail_customer_fact_sheet_v2`
        WHERE sfdc_account_id IS NOT NULL
    ),
    -- Products bought BEFORE visit
    pods_before AS (
        SELECT DISTINCT
            v.task_id,
            s.product_code
        FROM visits v
        JOIN account_vip_map m ON v.account_id = m.sfdc_account_id
        JOIN `artful-logic-475116-p1.analytics.vip_sales_clean` s
            ON m.vip_account_code = s.account_code
            AND m.distributor_code = s.distributor_code
            AND s.transaction_date >= DATE_SUB(v.visit_date, INTERVAL 30 DAY)
            AND s.transaction_date < v.visit_date
    ),
    -- Products bought AFTER visit
    pods_after AS (
        SELECT DISTINCT
            v.task_id,
            s.product_code
        FROM visits v
        JOIN account_vip_map m ON v.account_id = m.sfdc_account_id
        JOIN `artful-logic-475116-p1.analytics.vip_sales_clean` s
            ON m.vip_account_code = s.account_code
            AND m.distributor_code = s.distributor_code
            AND s.transaction_date > v.visit_date
            AND s.transaction_date <= DATE_ADD(v.visit_date, INTERVAL {attribution_window} DAY)
    ),
    -- NEW PODs = products in after but not in before
    new_pods AS (
        SELECT
            a.task_id,
            COUNT(DISTINCT a.product_code) as new_pod_count
        FROM pods_after a
        LEFT JOIN pods_before b ON a.task_id = b.task_id AND a.product_code = b.product_code
        WHERE b.product_code IS NULL  -- Not in before period
        GROUP BY a.task_id
    ),
    -- POD counts before/after for context
    pod_counts AS (
        SELECT
            v.task_id,
            COUNT(DISTINCT b.product_code) as pods_before,
            COUNT(DISTINCT a.product_code) as pods_after
        FROM visits v
        LEFT JOIN pods_before b ON v.task_id = b.task_id
        LEFT JOIN pods_after a ON v.task_id = a.task_id
        GROUP BY v.task_id
    )
    SELECT
        COUNT(DISTINCT v.task_id) as total_visits,
        COUNT(DISTINCT CASE WHEN np.new_pod_count > 0 THEN v.task_id END) as visits_with_new_pods,
        SAFE_DIVIDE(
            COUNT(DISTINCT CASE WHEN np.new_pod_count > 0 THEN v.task_id END),
            COUNT(DISTINCT v.task_id)
        ) * 100 as new_pod_rate,
        COALESCE(SUM(np.new_pod_count), 0) as total_new_pods,
        AVG(pc.pods_before) as avg_pods_before,
        AVG(pc.pods_after) as avg_pods_after
    FROM visits v
    LEFT JOIN new_pods np ON v.task_id = np.task_id
    LEFT JOIN pod_counts pc ON v.task_id = pc.task_id
    """
    return client.query(query).to_dataframe().iloc[0]


@st.cache_data(ttl=300)
def load_weekly_trend(weeks_back=12, _cache_version=CACHE_VERSION):
    """Load weekly visit attribution trend - before/after comparison model."""
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
          AND DATE(t.CompletedDateTime) >= '{ATTRIBUTION_START_DATE}'
    ),
    account_vip_map AS (
        SELECT DISTINCT
            sfdc_account_id,
            vip_id,
            vip_account_code,
            distributor_code
        FROM `artful-logic-475116-p1.staging_vip.retail_customer_fact_sheet_v2`
        WHERE sfdc_account_id IS NOT NULL
    ),
    -- Volume by product BEFORE visit
    volume_before AS (
        SELECT
            v.task_id,
            s.product_code,
            SUM(s.quantity) as units_before
        FROM visits v
        JOIN account_vip_map m ON v.account_id = m.sfdc_account_id
        JOIN `artful-logic-475116-p1.analytics.vip_sales_clean` s
            ON m.vip_account_code = s.account_code
            AND m.distributor_code = s.distributor_code
            AND s.transaction_date >= DATE_SUB(v.visit_date, INTERVAL 30 DAY)
            AND s.transaction_date < v.visit_date
        GROUP BY v.task_id, s.product_code
    ),
    -- Volume by product AFTER visit
    volume_after AS (
        SELECT
            v.task_id,
            s.product_code,
            SUM(s.quantity) as units_after
        FROM visits v
        JOIN account_vip_map m ON v.account_id = m.sfdc_account_id
        JOIN `artful-logic-475116-p1.analytics.vip_sales_clean` s
            ON m.vip_account_code = s.account_code
            AND m.distributor_code = s.distributor_code
            AND s.transaction_date > v.visit_date
            AND s.transaction_date <= DATE_ADD(v.visit_date, INTERVAL 30 DAY)
        GROUP BY v.task_id, s.product_code
    ),
    -- Calculate attribution per visit
    visit_attribution AS (
        SELECT
            COALESCE(a.task_id, b.task_id) as task_id,
            CASE WHEN b.units_before IS NULL AND a.units_after > 0 THEN a.units_after ELSE 0 END as new_pod_units,
            CASE WHEN b.units_before IS NOT NULL AND a.units_after > b.units_before
                 THEN a.units_after - b.units_before ELSE 0 END as incremental_units
        FROM volume_after a
        FULL OUTER JOIN volume_before b
            ON a.task_id = b.task_id AND a.product_code = b.product_code
        WHERE a.units_after IS NOT NULL
    ),
    -- Aggregate to visit level
    visit_totals AS (
        SELECT
            task_id,
            SUM(new_pod_units + incremental_units) as total_attributed_units
        FROM visit_attribution
        GROUP BY task_id
    )
    SELECT
        v.visit_week,
        COUNT(DISTINCT v.task_id) as total_visits,
        COUNT(DISTINCT CASE WHEN vt.total_attributed_units > 0 THEN v.task_id END) as visits_converted,
        SAFE_DIVIDE(
            COUNT(DISTINCT CASE WHEN vt.total_attributed_units > 0 THEN v.task_id END),
            COUNT(DISTINCT v.task_id)
        ) * 100 as conversion_rate,
        COALESCE(SUM(vt.total_attributed_units), 0) as total_attributed_units
    FROM visits v
    LEFT JOIN visit_totals vt ON v.task_id = vt.task_id
    WHERE v.visit_week >= '{ATTRIBUTION_START_DATE}'
      AND v.visit_week <= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
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


def render_leaderboard_entry(rank, name, visits, conversion_rate, units, measurable_visits=0, show_badges=True):
    import html
    rank_class = {1: "leaderboard-rank-gold", 2: "leaderboard-rank-silver", 3: "leaderboard-rank-bronze"}.get(rank, "")
    safe_name = html.escape(str(name)) if name else "Unknown"
    badges_html = ""
    if show_badges:
        if conversion_rate >= 50:
            badges_html += '<span class="attribution-badge">HIGH CONVERTER</span>'
        if units >= 500:
            badges_html += '<span class="pod-badge">TOP SELLER</span>'

    # BAN-style metrics row (using CSS classes for responsiveness)
    metrics_html = f'''
    <div class="ban-metrics">
        <div class="ban-metric">
            <div class="ban-value" style="color: #ffd666;">{int(visits)}</div>
            <div class="ban-label">Visits</div>
        </div>
        <div class="ban-metric">
            <div class="ban-value" style="color: #8892b0;">{int(measurable_visits)}</div>
            <div class="ban-label">Measurable</div>
        </div>
        <div class="ban-metric">
            <div class="ban-value" style="color: #64ffda;">{conversion_rate:.0f}%</div>
            <div class="ban-label">Converted</div>
        </div>
        <div class="ban-metric">
            <div class="ban-value" style="color: #ccd6f6;">{int(units):,}</div>
            <div class="ban-label">Units</div>
        </div>
    </div>
    '''
    return f'<div class="leaderboard-card"><span class="leaderboard-rank {rank_class}">#{rank}</span> <span class="leaderboard-name">{safe_name}</span>{badges_html}{metrics_html}</div>'


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
            index=1,  # Default to 14 days
            format_func=lambda x: f"{x} days post-visit"
        )

        # Load rep list for filter (we'll load full data later)
        # Use session state to avoid reloading rep list on every interaction
        if 'rep_list' not in st.session_state:
            try:
                rep_data = load_rep_performance(days_back, attribution_window)
                st.session_state.rep_list = ['All Reps'] + sorted(rep_data['rep_name'].tolist())
            except Exception:
                st.session_state.rep_list = ['All Reps']

        selected_rep = st.selectbox(
            "Select Rep",
            options=st.session_state.rep_list,
            index=0
        )

        st.markdown("---")
        st.markdown(f"""
        **Attribution Logic**

        **Bonus Period**: Nov 17, 2025 onwards

        A visit "converts" if the account shows **growth** within {attribution_window} days post-visit:

        - **New POD Units**: Volume from SKUs not bought in prior 30 days
        - **Incremental Units**: Volume increase on existing SKUs

        Only the **delta** (growth) is attributed, not total volume.

        *Note: Only visits {attribution_window}+ days old are measured (need full window to elapse).*
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
        bonus_visits = load_bonus_period_visits()
        attribution = load_visit_attribution(days_back, attribution_window, selected_rep)
        rep_performance = load_rep_performance(days_back, attribution_window)
        weekly_trend = load_weekly_trend(12)

        # Filter rep_performance if a specific rep is selected
        if selected_rep and selected_rep != "All Reps":
            rep_performance = rep_performance[rep_performance['rep_name'] == selected_rep]
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return

    # Section 1: EFFORT METRICS (Real-time)
    st.markdown('<p class="section-header">Effort Metrics (Real-Time)</p>', unsafe_allow_html=True)
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.markdown(render_metric_card(
            f"{int(bonus_visits['total_visits']):,}",
            "Total Visits",
            f"Since Nov 17, 2025",
            "neutral"
        ), unsafe_allow_html=True)

    with col2:
        st.markdown(render_metric_card(
            f"{int(bonus_visits['unique_accounts']):,}",
            "Unique Accounts",
            f"{int(bonus_visits['active_reps'])} active reps",
            "neutral"
        ), unsafe_allow_html=True)

    with col3:
        st.markdown(render_metric_card(
            f"{int(bonus_visits['measurable_visits']):,}",
            "Measurable Visits",
            f"14+ days old",
            "gold"
        ), unsafe_allow_html=True)

    with col4:
        st.markdown(render_metric_card(
            f"{int(bonus_visits['pending_visits']):,}",
            "Pending Attribution",
            f"< 14 days old",
            "neutral"
        ), unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # Section 2: ATTRIBUTION METRICS (Lagged - only measurable visits)
    st.markdown('<p class="section-header">Attribution Metrics (14-Day Lag)</p>', unsafe_allow_html=True)
    st.caption("⚠️ These metrics only include visits from Nov 17-18 (the only visits 14+ days old so far). More data will appear as time passes.")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        conv_rate = attribution['conversion_rate'] if pd.notna(attribution['conversion_rate']) else 0
        conv_status = "healthy" if conv_rate >= 20 else "gold" if conv_rate >= 10 else "critical"
        st.markdown(render_metric_card(
            f"{conv_rate:.1f}%",
            "Conversion Rate",
            f"{int(attribution['visits_converted']):,} visits with growth",
            conv_status
        ), unsafe_allow_html=True)

    with col2:
        new_pod_units = attribution['new_pod_units'] if pd.notna(attribution['new_pod_units']) else 0
        st.markdown(render_metric_card(
            f"{int(new_pod_units):,}",
            "New POD Units",
            f"{int(attribution['total_new_pods']):,} new SKUs added",
            "healthy"
        ), unsafe_allow_html=True)

    with col3:
        incremental_units = attribution['incremental_units'] if pd.notna(attribution['incremental_units']) else 0
        st.markdown(render_metric_card(
            f"{int(incremental_units):,}",
            "Incremental Units",
            "Growth on existing SKUs",
            "gold"
        ), unsafe_allow_html=True)

    with col4:
        total_attributed = attribution['total_attributed_units'] if pd.notna(attribution['total_attributed_units']) else 0
        st.markdown(render_metric_card(
            f"{int(total_attributed):,}",
            "Total Attributed",
            "New POD + Incremental",
            "neutral"
        ), unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # Row 2: Rep Leaderboard (Top 5 left, Bottom 5 right)
    st.markdown('<p class="section-header">Rep Leaderboard</p>', unsafe_allow_html=True)
    st.markdown('<p style="font-size: 0.75rem; color: #6b7280; margin-top: -8px; margin-bottom: 12px;">Conversion % and units are based on visits 14+ days old (measurable)</p>', unsafe_allow_html=True)
    if not rep_performance.empty:
        col1, col2 = st.columns(2)
        total_reps = len(rep_performance)
        top_5 = rep_performance.head(5)
        bottom_5 = rep_performance.tail(5) if total_reps > 5 else pd.DataFrame()

        # Top 5 on the left
        with col1:
            st.markdown('<p style="font-size: 0.9rem; color: #6b7280; margin-bottom: 0.5rem;">Top 5</p>', unsafe_allow_html=True)
            for rank, (idx, row) in enumerate(top_5.iterrows(), start=1):
                st.markdown(render_leaderboard_entry(
                    rank=rank,
                    name=row['rep_name'],
                    visits=row['total_visits'],
                    conversion_rate=row['conversion_rate'] if pd.notna(row['conversion_rate']) else 0,
                    units=row['total_attributed_units'] if pd.notna(row['total_attributed_units']) else 0,
                    measurable_visits=row['measurable_visits'] if pd.notna(row['measurable_visits']) else 0
                ), unsafe_allow_html=True)

        # Bottom 5 on the right
        with col2:
            if not bottom_5.empty:
                st.markdown('<p style="font-size: 0.9rem; color: #6b7280; margin-bottom: 0.5rem;">Bottom 5</p>', unsafe_allow_html=True)
                for rank, (idx, row) in enumerate(bottom_5.iterrows(), start=total_reps - 4):
                    st.markdown(render_leaderboard_entry(
                        rank=rank,
                        name=row['rep_name'],
                        visits=row['total_visits'],
                        conversion_rate=row['conversion_rate'] if pd.notna(row['conversion_rate']) else 0,
                        units=row['total_attributed_units'] if pd.notna(row['total_attributed_units']) else 0,
                        measurable_visits=row['measurable_visits'] if pd.notna(row['measurable_visits']) else 0
                    ), unsafe_allow_html=True)
    else:
        st.info("No rep data available")

    # Weekly trend section removed - data not reliable enough yet
    if False:  # Disabled for now
        st.markdown('<p class="section-header">Weekly Attribution Trend</p>', unsafe_allow_html=True)
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
                name='Visits w/ Growth',
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
        # Columns: rep_name, total_visits, unique_accounts, visits_converted, conversion_rate,
        #          new_pod_units, incremental_units, total_attributed_units, new_pods_count
        display_df = display_df[['rep_name', 'total_visits', 'unique_accounts', 'visits_converted',
                                  'conversion_rate', 'new_pod_units', 'incremental_units',
                                  'total_attributed_units', 'new_pods_count']]
        display_df.columns = ['Rep', 'Visits', 'Accounts', 'Converted', 'Conv %',
                              'New POD Units', 'Incremental', 'Total Attributed', 'New PODs']
        display_df['Conv %'] = display_df['Conv %'].apply(lambda x: f"{x:.1f}%" if pd.notna(x) else "0%")
        display_df['New POD Units'] = display_df['New POD Units'].apply(lambda x: f"{x:,.0f}" if pd.notna(x) else "0")
        display_df['Incremental'] = display_df['Incremental'].apply(lambda x: f"{x:,.0f}" if pd.notna(x) else "0")
        display_df['Total Attributed'] = display_df['Total Attributed'].apply(lambda x: f"{x:,.0f}" if pd.notna(x) else "0")
        display_df['New PODs'] = display_df['New PODs'].apply(lambda x: f"{x:,.0f}" if pd.notna(x) else "0")
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
