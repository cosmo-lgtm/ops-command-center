"""
Distributor Inventory Analysis Dashboard
Analyzes Salesforce orders vs VIP depletion to calculate weeks of inventory,
identify overstock/understock situations by distributor and product.
"""

import streamlit as st
import pandas as pd
import numpy as np
from google.cloud import bigquery
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go
from scipy import stats

# Dark mode custom CSS
st.markdown("""
<style>
    /* Force wide layout */
    .block-container {
        max-width: 100% !important;
        padding-left: 2rem !important;
        padding-right: 2rem !important;
    }

    /* Main background */
    .stApp {
        background: linear-gradient(135deg, #0f0f1a 0%, #1a1a2e 50%, #16213e 100%);
    }

    /* Hide Streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}

    /* Custom metric cards */
    .metric-card {
        background: linear-gradient(145deg, #1e1e2f 0%, #2a2a4a 100%);
        border-radius: 16px;
        padding: 24px;
        border: 1px solid rgba(255,255,255,0.1);
        box-shadow: 0 8px 32px rgba(0,0,0,0.3);
        transition: transform 0.3s ease, box-shadow 0.3s ease;
    }

    .metric-card:hover {
        transform: translateY(-4px);
        box-shadow: 0 12px 40px rgba(0,0,0,0.4);
    }

    .metric-value {
        font-size: 42px;
        font-weight: 700;
        background: linear-gradient(135deg, #00d4aa 0%, #00a3cc 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin: 0;
    }

    .metric-value-warning {
        font-size: 42px;
        font-weight: 700;
        background: linear-gradient(135deg, #ffd666 0%, #ff9f43 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin: 0;
    }

    .metric-value-danger {
        font-size: 42px;
        font-weight: 700;
        background: linear-gradient(135deg, #ff6b6b 0%, #ee5a5a 100%);
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

    .metric-delta-positive {
        color: #64ffda;
        font-size: 14px;
    }

    .metric-delta-negative {
        color: #ff6b6b;
        font-size: 14px;
    }

    /* Header styling */
    .dashboard-header {
        background: linear-gradient(90deg, #00d4aa 0%, #00a3cc 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-size: 48px;
        font-weight: 800;
        margin-bottom: 8px;
    }

    .dashboard-subtitle {
        color: #8892b0;
        font-size: 16px;
        margin-bottom: 32px;
    }

    /* Section headers */
    .section-header {
        color: #ccd6f6;
        font-size: 24px;
        font-weight: 600;
        margin: 32px 0 16px 0;
        padding-bottom: 8px;
        border-bottom: 2px solid rgba(0, 212, 170, 0.3);
    }

    /* Status badges */
    .status-overstock {
        background: rgba(255, 214, 102, 0.2);
        color: #ffd666;
        padding: 4px 12px;
        border-radius: 12px;
        font-weight: 600;
    }

    .status-understock {
        background: rgba(255, 107, 107, 0.2);
        color: #ff6b6b;
        padding: 4px 12px;
        border-radius: 12px;
        font-weight: 600;
    }

    .status-balanced {
        background: rgba(100, 255, 218, 0.2);
        color: #64ffda;
        padding: 4px 12px;
        border-radius: 12px;
        font-weight: 600;
    }

    /* Live indicator */
    .live-indicator {
        display: inline-flex;
        align-items: center;
        gap: 8px;
        color: #64ffda;
        font-size: 12px;
        text-transform: uppercase;
        letter-spacing: 1px;
    }

    .live-dot {
        width: 8px;
        height: 8px;
        background: #64ffda;
        border-radius: 50%;
        animation: pulse 2s infinite;
    }

    @keyframes pulse {
        0%, 100% { opacity: 1; transform: scale(1); }
        50% { opacity: 0.5; transform: scale(1.2); }
    }

    /* Scrollbar */
    ::-webkit-scrollbar {
        width: 8px;
        height: 8px;
    }

    ::-webkit-scrollbar-track {
        background: #1a1a2e;
    }

    ::-webkit-scrollbar-thumb {
        background: #00d4aa;
        border-radius: 4px;
    }

    /* Filter container */
    .filter-container {
        background: linear-gradient(145deg, #1e1e2f 0%, #2a2a4a 100%);
        border-radius: 16px;
        padding: 20px;
        margin-bottom: 24px;
        border: 1px solid rgba(255,255,255,0.1);
    }
</style>
""", unsafe_allow_html=True)

COLORS = {
    'primary': '#00d4aa',
    'secondary': '#00a3cc',
    'success': '#64ffda',
    'warning': '#ffd666',
    'danger': '#ff6b6b',
    'info': '#74b9ff',
    'gradient': ['#00d4aa', '#00a3cc', '#667eea', '#764ba2']
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
    # Handle secondary y-axis if provided
    if 'yaxis2' in kwargs:
        layout_args['yaxis2'] = {
            'gridcolor': 'rgba(255,255,255,0.1)',
            'linecolor': 'rgba(255,255,255,0.1)',
            **kwargs.get('yaxis2', {})
        }
    for k, v in kwargs.items():
        if k not in ['xaxis', 'yaxis', 'yaxis2', 'margin']:
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
    return bigquery.Client(project='artful-logic-475116-p1')


@st.cache_data(ttl=600)
def load_distributors():
    """Load list of distributors for the filter."""
    client = get_bq_client()
    query = """
    SELECT DISTINCT
        d.distributor_code,
        d.distributor_name,
        d.sfdc_distributor_account_id,
        CAST(d.total_retailers AS INT64) as total_retailers
    FROM `artful-logic-475116-p1.staging_vip.distributor_fact_sheet_v2` d
    WHERE d.distributor_code IS NOT NULL
    ORDER BY d.distributor_name
    """
    return client.query(query).to_dataframe()


@st.cache_data(ttl=300)
def load_inventory_data(lookback_days: int = 90):
    """
    Load comprehensive inventory data combining:
    - Salesforce orders (what we ship TO distributors) - ALL distributor orders
    - VIP depletion (what distributors sell THROUGH to retail) - rolled up via parent relationships

    This uses parent account rollup:
    1. VIP distributors link to SF child accounts (with VIP_ID__c)
    2. SF orders are placed on parent/HQ accounts
    3. We roll up VIP depletion from children to parent order accounts

    Inventory Health Calculation:
    - Order/Depletion Ratio > 1.3 = Overstock (building inventory faster than depleting)
    - Order/Depletion Ratio < 0.7 = Understock (depleting faster than restocking)
    - Ratio between 0.7 and 1.3 = Balanced
    """
    client = get_bq_client()

    query = f"""
    WITH
    -- Salesforce orders to distributors (last N days)
    -- Excludes Draft orders
    sf_orders AS (
        SELECT
            sfo.account_id,
            sfo.customer_name as distributor_name,
            SUM(CAST(sfo.quantity AS INT64)) as qty_ordered,
            SUM(CAST(sfo.line_total_price AS FLOAT64)) as order_value,
            COUNT(DISTINCT sfo.order_id) as order_count,
            MAX(sfo.order_date) as last_order_date
        FROM `artful-logic-475116-p1.staging_salesforce.salesforce_orders_flattened` sfo
        WHERE sfo.account_type = 'Distributor'
            AND sfo.order_status != 'Draft'
            AND sfo.order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL {lookback_days} DAY)
            AND sfo.order_date <= CURRENT_DATE()
        GROUP BY account_id, customer_name
    ),

    -- VIP depletion by distributor (last N days)
    -- Uses cleaned/deduplicated sales data from analytics.vip_sales_clean
    vip_depletion AS (
        SELECT
            sc.distributor_code,
            SUM(sc.quantity) as qty_depleted,
            COUNT(DISTINCT sc.account_code) as stores_reached,
            COUNT(*) as transaction_count,
            SUM(sc.quantity) / ({lookback_days} / 7.0) as weekly_depletion_rate
        FROM `artful-logic-475116-p1.analytics.vip_sales_clean` sc
        WHERE sc.transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL {lookback_days} DAY)
        GROUP BY sc.distributor_code
    ),

    -- VIP distributor to SF account mapping (child accounts with VIP codes)
    -- Also get the parent account ID for rollup
    vip_to_sf AS (
        SELECT
            v.distributor_code,
            v.distributor_name as vip_dist_name,
            v.sfdc_distributor_account_id as sf_child_id,
            a.ParentId as sf_parent_id,
            CAST(v.total_retailers AS INT64) as total_retailers
        FROM `artful-logic-475116-p1.staging_vip.distributor_fact_sheet_v2` v
        LEFT JOIN `artful-logic-475116-p1.raw_salesforce.Account` a
            ON v.sfdc_distributor_account_id = a.Id
    ),

    -- Roll up VIP depletion to SF order accounts
    -- Match either: direct (order account = VIP account) OR parent (order account = parent of VIP account)
    sf_with_rollup AS (
        SELECT
            sfo.account_id,
            sfo.distributor_name,
            sfo.qty_ordered,
            sfo.order_value,
            sfo.order_count,
            STRING_AGG(DISTINCT vtf.distributor_code, ', ') as vip_codes,
            SUM(vd.qty_depleted) as total_qty_depleted,
            SUM(vd.stores_reached) as unique_stores,
            SUM(vd.transaction_count) as depletion_transactions,
            SUM(vd.weekly_depletion_rate) as weekly_depletion_rate,
            MAX(vtf.total_retailers) as total_retailers,
            CASE WHEN COUNT(vtf.distributor_code) > 0 THEN TRUE ELSE FALSE END as has_vip_match
        FROM sf_orders sfo
        LEFT JOIN vip_to_sf vtf
            ON sfo.account_id = vtf.sf_child_id  -- Direct match
            OR sfo.account_id = vtf.sf_parent_id  -- Parent rollup
        LEFT JOIN vip_depletion vd
            ON vtf.distributor_code = vd.distributor_code
        GROUP BY sfo.account_id, sfo.distributor_name, sfo.qty_ordered, sfo.order_value, sfo.order_count
    ),

    -- VIP codes that are already matched to SF orders (via direct or parent)
    matched_vip_codes AS (
        SELECT DISTINCT vtf.distributor_code
        FROM sf_orders sfo
        JOIN vip_to_sf vtf
            ON sfo.account_id = vtf.sf_child_id
            OR sfo.account_id = vtf.sf_parent_id
    ),

    -- VIP-only distributors (have depletion but no SF orders in period)
    vip_only AS (
        SELECT
            vtf.distributor_code as account_id,
            vtf.vip_dist_name as distributor_name,
            0 as qty_ordered,
            0.0 as order_value,
            0 as order_count,
            vtf.distributor_code as vip_codes,
            vd.qty_depleted as total_qty_depleted,
            vd.stores_reached as unique_stores,
            vd.transaction_count as depletion_transactions,
            vd.weekly_depletion_rate,
            vtf.total_retailers,
            TRUE as has_vip_match
        FROM vip_to_sf vtf
        JOIN vip_depletion vd ON vtf.distributor_code = vd.distributor_code
        LEFT JOIN matched_vip_codes mvc ON vtf.distributor_code = mvc.distributor_code
        WHERE mvc.distributor_code IS NULL
    ),

    -- Combine both sources
    combined AS (
        SELECT * FROM sf_with_rollup
        UNION ALL
        SELECT * FROM vip_only
    )

    SELECT
        account_id as distributor_code,
        distributor_name,
        account_id as sfdc_distributor_account_id,
        COALESCE(total_retailers, 0) as total_retailers,
        qty_ordered as total_qty_ordered,
        order_value as total_order_value,
        order_count as total_orders,
        COALESCE(total_qty_depleted, 0) as total_qty_depleted,
        COALESCE(unique_stores, 0) as unique_stores,
        COALESCE(depletion_transactions, 0) as depletion_transactions,
        COALESCE(weekly_depletion_rate, 0) as weekly_depletion_rate,
        has_vip_match,
        vip_codes,

        -- Order/Depletion Ratio
        CASE
            WHEN COALESCE(total_qty_depleted, 0) > 0
            THEN ROUND(qty_ordered * 1.0 / total_qty_depleted, 2)
            ELSE NULL
        END as order_depletion_ratio,

        -- Weeks of Inventory
        CASE
            WHEN COALESCE(weekly_depletion_rate, 0) > 0
            THEN ROUND(qty_ordered / weekly_depletion_rate, 1)
            ELSE NULL
        END as weeks_of_inventory,

        -- Inventory status
        CASE
            WHEN COALESCE(total_qty_depleted, 0) = 0 AND qty_ordered > 0 THEN 'No Depletion Data'
            WHEN qty_ordered = 0 AND COALESCE(total_qty_depleted, 0) > 0 THEN 'No Recent Orders'
            WHEN COALESCE(total_qty_depleted, 0) > 0 AND (qty_ordered * 1.0 / total_qty_depleted) > 1.3 THEN 'Overstock'
            WHEN COALESCE(total_qty_depleted, 0) > 0 AND (qty_ordered * 1.0 / total_qty_depleted) < 0.7 THEN 'Understock'
            WHEN COALESCE(total_qty_depleted, 0) > 0 THEN 'Balanced'
            ELSE 'No Data'
        END as inventory_status

    FROM combined
    ORDER BY order_value DESC
    """
    return client.query(query).to_dataframe()


@st.cache_data(ttl=300)
def load_product_level_data(distributor_codes: list = None, lookback_days: int = 90):
    """Load product-level inventory data for selected distributors."""
    client = get_bq_client()

    distributor_filter = ""
    if distributor_codes and len(distributor_codes) > 0:
        codes_str = "', '".join(distributor_codes)
        distributor_filter = f"AND d.distributor_code IN ('{codes_str}')"

    # Join with items master to get product names
    # Note: `Desc` is a reserved word so must be escaped with backticks
    query = f"""
    WITH items_deduped AS (
        SELECT
            SupplierItem as item_code,
            `Desc` as item_description,
            BrandDesc
        FROM `artful-logic-475116-p1.raw_vip.items`
        WHERE SupplierItem IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (PARTITION BY SupplierItem ORDER BY _airbyte_extracted_at DESC) = 1
    )
    -- Uses cleaned/deduplicated sales data from analytics.vip_sales_clean
    SELECT
        d.distributor_code,
        d.distributor_name,
        sc.product_code as Item_Code,
        COALESCE(i.item_description, sc.product_code) as product_name,
        i.BrandDesc as brand,
        SUM(sc.quantity) as qty_depleted,
        COUNT(DISTINCT sc.account_code) as stores_reached,
        COUNT(*) as transaction_count,
        ROUND(SUM(sc.quantity) / ({lookback_days} / 7.0), 1) as weekly_depletion_rate,
        CASE
            WHEN SUM(sc.quantity) / ({lookback_days} / 7.0) >= 10 THEN 'High Velocity'
            WHEN SUM(sc.quantity) / ({lookback_days} / 7.0) >= 3 THEN 'Medium Velocity'
            ELSE 'Low Velocity'
        END as velocity_status
    FROM `artful-logic-475116-p1.analytics.vip_sales_clean` sc
    JOIN `artful-logic-475116-p1.staging_vip.distributor_fact_sheet_v2` d
        ON sc.distributor_code = d.distributor_code
    LEFT JOIN items_deduped i
        ON sc.product_code = i.item_code
    WHERE sc.transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL {lookback_days} DAY)
        {distributor_filter}
    GROUP BY d.distributor_code, d.distributor_name, sc.product_code, i.item_description, i.BrandDesc
    ORDER BY d.distributor_name, qty_depleted DESC
    """
    return client.query(query).to_dataframe()


@st.cache_data(ttl=300)
def load_state_depletion_data(lookback_days: int = 90):
    """Load depletion data aggregated by state for US map visualization."""
    client = get_bq_client()

    query = f"""
    -- Uses cleaned/deduplicated sales data from analytics.vip_sales_clean
    -- POD = Points of Distribution = unique (door, SKU) combinations
    WITH state_depletion AS (
        SELECT
            d.state,
            d.distributor_code,
            SUM(sc.quantity) as qty_depleted,
            COUNT(DISTINCT sc.account_code) as stores_reached,
            COUNT(DISTINCT CONCAT(sc.account_code, '|', sc.product_code)) as pod_count
        FROM `artful-logic-475116-p1.analytics.vip_sales_clean` sc
        JOIN `artful-logic-475116-p1.staging_vip.distributor_fact_sheet_v2` d
            ON sc.distributor_code = d.distributor_code
        WHERE sc.transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL {lookback_days} DAY)
            AND d.state IS NOT NULL
            AND LENGTH(d.state) = 2
        GROUP BY d.state, d.distributor_code
    )
    SELECT
        state,
        COUNT(DISTINCT distributor_code) as distributor_count,
        SUM(qty_depleted) as total_depleted,
        SUM(stores_reached) as total_doors,
        SUM(pod_count) as total_pods,
        ROUND(SUM(pod_count) * 1.0 / NULLIF(SUM(stores_reached), 0), 1) as avg_pods_per_dist,
        ROUND(SUM(qty_depleted) / ({lookback_days} / 7.0), 0) as weekly_rate
    FROM state_depletion
    GROUP BY state
    ORDER BY total_depleted DESC
    """
    return client.query(query).to_dataframe()


@st.cache_data(ttl=300)
def load_trend_data(lookback_weeks: int = 12):
    """Load weekly trend data for orders and depletion."""
    client = get_bq_client()

    query = f"""
    WITH
    -- Weekly SF orders (excludes Draft orders)
    weekly_orders AS (
        SELECT
            DATE_TRUNC(order_date, WEEK) as week_start,
            SUM(CAST(quantity AS INT64)) as qty_ordered,
            SUM(CAST(line_total_price AS FLOAT64)) as order_value,
            COUNT(DISTINCT order_id) as order_count
        FROM `artful-logic-475116-p1.staging_salesforce.salesforce_orders_flattened`
        WHERE account_type = 'Distributor'
            AND order_status != 'Draft'
            AND order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL {lookback_weeks} WEEK)
            AND order_date <= CURRENT_DATE()
        GROUP BY week_start
    ),

    -- Weekly VIP depletion (uses cleaned/deduplicated sales data)
    weekly_depletion AS (
        SELECT
            DATE_TRUNC(transaction_date, WEEK) as week_start,
            SUM(quantity) as qty_depleted,
            COUNT(DISTINCT account_code) as stores_reached
        FROM `artful-logic-475116-p1.analytics.vip_sales_clean`
        WHERE transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL {lookback_weeks} WEEK)
        GROUP BY week_start
    )

    SELECT
        COALESCE(wo.week_start, wd.week_start) as week_start,
        COALESCE(wo.qty_ordered, 0) as qty_ordered,
        COALESCE(wo.order_value, 0) as order_value,
        COALESCE(wo.order_count, 0) as order_count,
        COALESCE(wd.qty_depleted, 0) as qty_depleted,
        COALESCE(wd.stores_reached, 0) as stores_reached
    FROM weekly_orders wo
    FULL OUTER JOIN weekly_depletion wd ON wo.week_start = wd.week_start
    WHERE COALESCE(wo.week_start, wd.week_start) IS NOT NULL
    ORDER BY week_start
    """
    return client.query(query).to_dataframe()


def _forecast_single_series(y: np.ndarray, forecast_weeks: int = 12):
    """
    Generate forecast for a single time series using damped trend approach.
    Trends regress toward the mean over time (more realistic for sales data).
    """
    n = len(y)

    # Calculate key statistics
    overall_mean = np.mean(y)
    recent_mean = np.mean(y[-4:]) if n >= 4 else overall_mean  # Last 4 weeks

    # Calculate recent trend (last 4 weeks vs previous 4 weeks)
    if n >= 8:
        recent_4wk = np.mean(y[-4:])
        previous_4wk = np.mean(y[-8:-4])
        weekly_trend = (recent_4wk - previous_4wk) / 4
    elif n >= 4:
        weekly_trend = (y[-1] - y[-4]) / 4
    else:
        weekly_trend = 0

    # Damping factor - trend decays toward zero over time
    # After ~8 weeks, trend influence is halved
    damping = 0.92

    # Generate forecast with damped trend (regresses to recent mean)
    forecast = np.zeros(forecast_weeks)
    current_level = recent_mean
    current_trend = weekly_trend

    for i in range(forecast_weeks):
        # Trend decays each week
        current_trend = current_trend * damping
        # Level moves toward overall mean slowly
        current_level = current_level + current_trend + 0.05 * (overall_mean - current_level)
        forecast[i] = current_level

    # Ensure non-negative and set floor at 50% of recent mean
    floor = recent_mean * 0.5
    forecast = np.maximum(forecast, floor)

    # === Confidence Intervals ===
    cv = np.std(y) / overall_mean if overall_mean > 0 else 0.15
    cv = min(cv, 0.25)  # Cap at 25%

    # Uncertainty grows with horizon
    horizon_factor = 1 + 0.1 * np.sqrt(np.arange(1, forecast_weeks + 1))
    uncertainty = cv * horizon_factor * forecast

    ci_80_lower = np.maximum(forecast - 0.8 * uncertainty, floor * 0.8)
    ci_80_upper = forecast + 0.8 * uncertainty
    ci_95_lower = np.maximum(forecast - 1.2 * uncertainty, floor * 0.6)
    ci_95_upper = forecast + 1.2 * uncertainty

    return {
        'ensemble': forecast,
        'lr': forecast,  # Keep for compatibility
        'es': forecast,
        'mat': forecast,
        'ci_80_lower': ci_80_lower,
        'ci_80_upper': ci_80_upper,
        'ci_95_lower': ci_95_lower,
        'ci_95_upper': ci_95_upper
    }


def generate_ensemble_forecast(trend_df: pd.DataFrame, forecast_weeks: int = 12):
    """
    Generate 3-month (12-week) forecast for both orders and depletion using ensemble of 3 models:
    1. Linear Regression
    2. Exponential Smoothing (Simple)
    3. Moving Average with Trend

    Returns forecast dataframe with ensemble predictions and confidence intervals for both series.
    """
    if len(trend_df) < 4:
        return None

    # Sort by week and prepare data
    df = trend_df.sort_values('week_start').copy()

    # Need at least 4 weeks of data
    df_clean = df.dropna(subset=['qty_depleted', 'order_value'])
    if len(df_clean) < 4:
        # Try with just non-zero values
        df_clean = df[(df['qty_depleted'] > 0) | (df['order_value'] > 0)]
        if len(df_clean) < 4:
            return None

    # Historical values - use order_value (dollars) instead of qty_ordered (units)
    y_depletion = df_clean['qty_depleted'].fillna(0).values
    y_orders = df_clean['order_value'].fillna(0).values

    # Future weeks
    future_weeks = pd.date_range(
        start=df_clean['week_start'].max() + timedelta(weeks=1),
        periods=forecast_weeks,
        freq='W-SUN'
    )

    # Generate forecasts for both series
    depletion_fc = _forecast_single_series(y_depletion, forecast_weeks)
    orders_fc = _forecast_single_series(y_orders, forecast_weeks)

    # Build forecast dataframe
    forecast_df = pd.DataFrame({
        'week_start': future_weeks,
        'is_forecast': True,
        # Depletion forecast
        'depletion_ensemble': depletion_fc['ensemble'],
        'depletion_lr': depletion_fc['lr'],
        'depletion_es': depletion_fc['es'],
        'depletion_mat': depletion_fc['mat'],
        'depletion_ci_80_lower': depletion_fc['ci_80_lower'],
        'depletion_ci_80_upper': depletion_fc['ci_80_upper'],
        'depletion_ci_95_lower': depletion_fc['ci_95_lower'],
        'depletion_ci_95_upper': depletion_fc['ci_95_upper'],
        # Orders forecast
        'orders_ensemble': orders_fc['ensemble'],
        'orders_lr': orders_fc['lr'],
        'orders_es': orders_fc['es'],
        'orders_mat': orders_fc['mat'],
        'orders_ci_80_lower': orders_fc['ci_80_lower'],
        'orders_ci_80_upper': orders_fc['ci_80_upper'],
        'orders_ci_95_lower': orders_fc['ci_95_lower'],
        'orders_ci_95_upper': orders_fc['ci_95_upper'],
    })

    # Historical data for plotting continuity - use order_value for dollars
    historical_df = df_clean[['week_start', 'qty_depleted', 'order_value']].copy()
    historical_df['is_forecast'] = False

    return forecast_df, historical_df


def render_metric_card(value, label, card_type="primary"):
    """Render a styled metric card."""
    value_class = "metric-value"
    if card_type == "warning":
        value_class = "metric-value-warning"
    elif card_type == "danger":
        value_class = "metric-value-danger"

    return f"""
    <div class="metric-card">
        <div class="{value_class}">{value}</div>
        <div class="metric-label">{label}</div>
    </div>
    """


def main():
    # Header
    st.markdown("""
    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 32px;">
        <div>
            <h1 class="dashboard-header">Distributor Inventory Analysis</h1>
            <p class="dashboard-subtitle">Orders vs Depletion - Weeks of Inventory & Stock Status</p>
        </div>
        <div class="live-indicator">
            <span class="live-dot"></span>
            Live Data
        </div>
    </div>
    """, unsafe_allow_html=True)

    # Load data
    try:
        distributors_df = load_distributors()
        inventory_df = load_inventory_data(lookback_days=90)
        trend_df = load_trend_data(lookback_weeks=12)
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return

    # Filter Section
    col1, col2, col3 = st.columns([3, 1, 1])

    with col1:
        # Distributor multiselect
        distributor_options = ["All Distributors"] + sorted(inventory_df['distributor_name'].dropna().unique().tolist())
        selected_distributors = st.multiselect(
            "Select Distributors",
            options=distributor_options,
            default=["All Distributors"],
            help="Select one or more distributors to filter the analysis"
        )

    with col2:
        lookback_days = st.selectbox(
            "Lookback Period",
            options=[30, 60, 90, 180],
            index=2,
            format_func=lambda x: f"{x} days"
        )

    with col3:
        inventory_threshold = st.selectbox(
            "Overstock Threshold",
            options=[8, 10, 12, 16],
            index=2,
            format_func=lambda x: f"{x} weeks"
        )

    # Filter the data based on selection
    if "All Distributors" not in selected_distributors and len(selected_distributors) > 0:
        filtered_df = inventory_df[inventory_df['distributor_name'].isin(selected_distributors)]
    else:
        filtered_df = inventory_df.copy()

    # Calculate summary stats
    total_distributors = len(filtered_df)
    total_order_value = filtered_df['total_order_value'].sum()
    total_qty_ordered = filtered_df['total_qty_ordered'].sum()
    total_qty_depleted = filtered_df['total_qty_depleted'].sum()

    # Status counts
    overstock_count = len(filtered_df[filtered_df['inventory_status'] == 'Overstock'])
    understock_count = len(filtered_df[filtered_df['inventory_status'] == 'Understock'])
    balanced_count = len(filtered_df[filtered_df['inventory_status'] == 'Balanced'])
    no_depletion_count = len(filtered_df[filtered_df['inventory_status'] == 'No Depletion Data'])
    no_orders_count = len(filtered_df[filtered_df['inventory_status'] == 'No Recent Orders'])
    vip_matched = len(filtered_df[filtered_df['has_vip_match'] == True])

    avg_weeks = filtered_df[filtered_df['weeks_of_inventory'].notna() & (filtered_df['weeks_of_inventory'] > 0)]['weeks_of_inventory'].mean()

    # KPI Cards Row
    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        st.markdown(render_metric_card(
            f"{total_distributors:,}",
            "Active Distributors"
        ), unsafe_allow_html=True)

    with col2:
        st.markdown(render_metric_card(
            f"${total_order_value/1000000:.1f}M",
            "Order Value (90d)"
        ), unsafe_allow_html=True)

    with col3:
        overstock_pct = round(100 * overstock_count / max(total_distributors, 1), 1)
        st.markdown(render_metric_card(
            f"{overstock_count} ({overstock_pct}%)",
            "Overstocked",
            card_type="warning"
        ), unsafe_allow_html=True)

    with col4:
        understock_pct = round(100 * understock_count / max(total_distributors, 1), 1)
        st.markdown(render_metric_card(
            f"{understock_count} ({understock_pct}%)",
            "Understocked",
            card_type="danger"
        ), unsafe_allow_html=True)

    with col5:
        avg_weeks_display = f"{avg_weeks:.1f}" if pd.notna(avg_weeks) else "N/A"
        st.markdown(render_metric_card(
            avg_weeks_display,
            "Avg Weeks Inventory"
        ), unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # Charts Row 1: Trend + Status Distribution
    col1, col2 = st.columns([2, 1])

    with col1:
        st.markdown('<p class="section-header">Units Ordered vs Depleted</p>', unsafe_allow_html=True)

        if not trend_df.empty:
            trend_sorted = trend_df.sort_values('week_start')

            # Calculate 4-week moving averages (both in units)
            trend_sorted['orders_ma'] = trend_sorted['qty_ordered'].rolling(window=4, min_periods=2).mean()
            trend_sorted['depletion_ma'] = trend_sorted['qty_depleted'].rolling(window=4, min_periods=2).mean()

            fig = go.Figure()

            # Raw data lines - both in units
            fig.add_trace(go.Scatter(
                x=trend_sorted['week_start'],
                y=trend_sorted['qty_ordered'],
                mode='lines+markers',
                name='Units Ordered (SF)',
                line=dict(color=COLORS['primary'], width=2),
                marker=dict(size=6),
                opacity=0.7
            ))

            fig.add_trace(go.Scatter(
                x=trend_sorted['week_start'],
                y=trend_sorted['qty_depleted'],
                mode='lines+markers',
                name='Units Depleted (VIP)',
                line=dict(color=COLORS['secondary'], width=2),
                marker=dict(size=6),
                opacity=0.7
            ))

            # Moving average lines (dashed for distinction)
            fig.add_trace(go.Scatter(
                x=trend_sorted['week_start'],
                y=trend_sorted['orders_ma'],
                mode='lines',
                name='Orders 4-wk MA',
                line=dict(color=COLORS['primary'], width=3, dash='dash'),
            ))

            fig.add_trace(go.Scatter(
                x=trend_sorted['week_start'],
                y=trend_sorted['depletion_ma'],
                mode='lines',
                name='Depletion 4-wk MA',
                line=dict(color=COLORS['secondary'], width=3, dash='dash'),
            ))

            apply_dark_theme(fig, height=350,
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font=dict(color='#8892b0')),
                hovermode='x unified',
                yaxis=dict(title=dict(text='Units', font=dict(color='#ccd6f6')), tickfont=dict(color='#ccd6f6'), tickformat=',.0f')
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No trend data available")

    with col2:
        st.markdown('<p class="section-header">Distributor Inventory Health</p>', unsafe_allow_html=True)

        # Count distributors with and without depletion data
        has_depletion = filtered_df[
            (filtered_df['total_qty_depleted'].notna()) &
            (filtered_df['total_qty_depleted'] > 0)
        ]
        no_depletion = filtered_df[
            (filtered_df['total_qty_depleted'].isna()) |
            (filtered_df['total_qty_depleted'] == 0)
        ]

        # For those with depletion, categorize by weeks of inventory
        understock_count = len(has_depletion[has_depletion['weeks_of_inventory'] < 4])
        balanced_count_chart = len(has_depletion[
            (has_depletion['weeks_of_inventory'] >= 4) &
            (has_depletion['weeks_of_inventory'] <= 12)
        ])
        overstock_count_chart = len(has_depletion[has_depletion['weeks_of_inventory'] > 12])
        no_depletion_count_chart = len(no_depletion)

        fig = go.Figure()

        # Stacked bar: With Depletion Data (broken down by health)
        fig.add_trace(go.Bar(
            name='Understock (<4 wks)',
            x=['With Depletion'],
            y=[understock_count],
            marker_color=COLORS['danger'],
            text=[understock_count] if understock_count > 0 else None,
            textposition='inside',
            textfont=dict(color='white', size=14)
        ))

        fig.add_trace(go.Bar(
            name='Balanced (4-12 wks)',
            x=['With Depletion'],
            y=[balanced_count_chart],
            marker_color=COLORS['success'],
            text=[balanced_count_chart] if balanced_count_chart > 0 else None,
            textposition='inside',
            textfont=dict(color='white', size=14)
        ))

        fig.add_trace(go.Bar(
            name='Overstock (>12 wks)',
            x=['With Depletion'],
            y=[overstock_count_chart],
            marker_color=COLORS['warning'],
            text=[overstock_count_chart] if overstock_count_chart > 0 else None,
            textposition='inside',
            textfont=dict(color='white', size=14)
        ))

        # Second bar: No Depletion Data
        fig.add_trace(go.Bar(
            name='No Depletion Data',
            x=['No Depletion'],
            y=[no_depletion_count_chart],
            marker_color='#8892b0',
            text=[no_depletion_count_chart] if no_depletion_count_chart > 0 else None,
            textposition='inside',
            textfont=dict(color='white', size=14),
            showlegend=True
        ))

        apply_dark_theme(fig, height=350)
        fig.update_layout(
            barmode='stack',
            yaxis_title="# Distributors",
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="center",
                x=0.5,
                font=dict(color='#8892b0', size=10)
            ),
            xaxis=dict(tickfont=dict(color='#ccd6f6'))
        )
        st.plotly_chart(fig, use_container_width=True)

    # Forecast Section: 3-Month Orders & Depletion Forecast with Cone of Certainty
    st.markdown('<p class="section-header">📈 3-Month Forecast</p>', unsafe_allow_html=True)
    st.markdown('<p style="color: #8892b0; font-size: 14px; margin-top: -10px;">Ensemble forecast using Linear Regression, Exponential Smoothing, and Moving Average with Trend</p>', unsafe_allow_html=True)

    if not trend_df.empty:
        forecast_result = generate_ensemble_forecast(trend_df, forecast_weeks=12)

        if forecast_result is not None:
            forecast_df, historical_df = forecast_result

            # Two side-by-side charts: Orders forecast and Depletion forecast
            fchart1, fchart2 = st.columns(2)

            with fchart1:
                st.markdown('<p style="color: #ccd6f6; font-size: 16px; font-weight: 600;">Orders Forecast</p>', unsafe_allow_html=True)
                fig_orders = go.Figure()

                # Historical orders line (in $K for readability)
                hist_orders = historical_df['order_value'] / 1000
                fig_orders.add_trace(go.Scatter(
                    x=historical_df['week_start'],
                    y=hist_orders,
                    mode='lines+markers',
                    name='Historical Orders',
                    line=dict(color=COLORS['primary'], width=3),
                    marker=dict(size=6),
                    hovertemplate='$%{y:,.0f}K<extra></extra>'
                ))

                # 4-week moving average (dotted line) - Orders
                orders_ma = hist_orders.rolling(window=4, min_periods=2).mean()
                fig_orders.add_trace(go.Scatter(
                    x=historical_df['week_start'],
                    y=orders_ma,
                    mode='lines',
                    name='4-wk MA',
                    line=dict(color=COLORS['primary'], width=2, dash='dot'),
                    hovertemplate='$%{y:,.0f}K<extra></extra>'
                ))

                # 95% Confidence interval (outer cone) - Orders (in $K)
                fig_orders.add_trace(go.Scatter(
                    x=pd.concat([forecast_df['week_start'], forecast_df['week_start'][::-1]]),
                    y=pd.concat([forecast_df['orders_ci_95_upper'] / 1000, forecast_df['orders_ci_95_lower'][::-1] / 1000]),
                    fill='toself',
                    fillcolor='rgba(102, 126, 234, 0.15)',
                    line=dict(color='rgba(0,0,0,0)'),
                    name='95% CI',
                    showlegend=True,
                    hoverinfo='skip'
                ))

                # 80% Confidence interval (inner cone) - Orders (in $K)
                fig_orders.add_trace(go.Scatter(
                    x=pd.concat([forecast_df['week_start'], forecast_df['week_start'][::-1]]),
                    y=pd.concat([forecast_df['orders_ci_80_upper'] / 1000, forecast_df['orders_ci_80_lower'][::-1] / 1000]),
                    fill='toself',
                    fillcolor='rgba(102, 126, 234, 0.3)',
                    line=dict(color='rgba(0,0,0,0)'),
                    name='80% CI',
                    showlegend=True,
                    hoverinfo='skip'
                ))

                # Ensemble forecast line - Orders (in $K)
                fig_orders.add_trace(go.Scatter(
                    x=forecast_df['week_start'],
                    y=forecast_df['orders_ensemble'] / 1000,
                    mode='lines+markers',
                    name='Forecast',
                    line=dict(color=COLORS['primary'], width=3),
                    marker=dict(size=6, symbol='diamond'),
                    hovertemplate='$%{y:,.0f}K<extra></extra>'
                ))

                # Vertical line for forecast start
                last_hist_str = str(historical_df['week_start'].max())[:10]
                fig_orders.add_vline(
                    x=last_hist_str,
                    line_dash="dash",
                    line_color="#8892b0"
                )

                apply_dark_theme(fig_orders, height=300,
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font=dict(color='#8892b0', size=9)),
                    hovermode='x unified'
                )
                fig_orders.update_layout(
                    yaxis_title="Order Value ($K)",
                    yaxis=dict(tickformat='$,.0f')
                )
                st.plotly_chart(fig_orders, use_container_width=True)

            with fchart2:
                st.markdown('<p style="color: #ccd6f6; font-size: 16px; font-weight: 600;">Depletion Forecast</p>', unsafe_allow_html=True)
                fig_depl = go.Figure()

                # Historical depletion line
                hist_depletion = historical_df['qty_depleted']
                fig_depl.add_trace(go.Scatter(
                    x=historical_df['week_start'],
                    y=hist_depletion,
                    mode='lines+markers',
                    name='Historical Depletion',
                    line=dict(color=COLORS['secondary'], width=3),
                    marker=dict(size=6)
                ))

                # 4-week moving average (dotted line) - Depletion
                depletion_ma = hist_depletion.rolling(window=4, min_periods=2).mean()
                fig_depl.add_trace(go.Scatter(
                    x=historical_df['week_start'],
                    y=depletion_ma,
                    mode='lines',
                    name='4-wk MA',
                    line=dict(color=COLORS['secondary'], width=2, dash='dot')
                ))

                # 95% Confidence interval (outer cone) - Depletion
                fig_depl.add_trace(go.Scatter(
                    x=pd.concat([forecast_df['week_start'], forecast_df['week_start'][::-1]]),
                    y=pd.concat([forecast_df['depletion_ci_95_upper'], forecast_df['depletion_ci_95_lower'][::-1]]),
                    fill='toself',
                    fillcolor='rgba(0, 212, 170, 0.15)',
                    line=dict(color='rgba(0,0,0,0)'),
                    name='95% CI',
                    showlegend=True,
                    hoverinfo='skip'
                ))

                # 80% Confidence interval (inner cone) - Depletion
                fig_depl.add_trace(go.Scatter(
                    x=pd.concat([forecast_df['week_start'], forecast_df['week_start'][::-1]]),
                    y=pd.concat([forecast_df['depletion_ci_80_upper'], forecast_df['depletion_ci_80_lower'][::-1]]),
                    fill='toself',
                    fillcolor='rgba(0, 212, 170, 0.3)',
                    line=dict(color='rgba(0,0,0,0)'),
                    name='80% CI',
                    showlegend=True,
                    hoverinfo='skip'
                ))

                # Ensemble forecast line - Depletion
                fig_depl.add_trace(go.Scatter(
                    x=forecast_df['week_start'],
                    y=forecast_df['depletion_ensemble'],
                    mode='lines+markers',
                    name='Forecast',
                    line=dict(color=COLORS['secondary'], width=3),
                    marker=dict(size=6, symbol='diamond')
                ))

                # Vertical line for forecast start
                fig_depl.add_vline(
                    x=last_hist_str,
                    line_dash="dash",
                    line_color="#8892b0"
                )

                apply_dark_theme(fig_depl, height=300,
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font=dict(color='#8892b0', size=9)),
                    hovermode='x unified'
                )
                fig_depl.update_layout(yaxis_title="Units Depleted")
                st.plotly_chart(fig_depl, use_container_width=True)

            # Forecast summary metrics - side by side: Orders (left) | Depletion (right)
            mcol1, mcol2 = st.columns(2)

            with mcol1:
                st.markdown('<p style="color: #ccd6f6; font-size: 14px; margin-top: 10px;">📦 <b>Orders Forecast</b></p>', unsafe_allow_html=True)
                ocol1, ocol2 = st.columns(2)
                with ocol1:
                    o_first = forecast_df.head(6)['orders_ensemble'].mean()
                    o_second = forecast_df.tail(6)['orders_ensemble'].mean()
                    o_trend = ((o_second - o_first) / o_first * 100) if o_first > 0 else 0
                    o_label = "Trending Up" if o_trend > 5 else ("Trending Down" if o_trend < -5 else "Stable")
                    o_type = "primary" if o_trend > 5 else ("danger" if o_trend < -5 else "warning")
                    st.markdown(render_metric_card(f"{o_trend:+.1f}%", o_label, o_type), unsafe_allow_html=True)
                with ocol2:
                    orders_4wk = forecast_df.head(4)['orders_ensemble'].sum()
                    st.markdown(render_metric_card(f"${orders_4wk:,.0f}", "Next 4 Weeks", "primary"), unsafe_allow_html=True)

                ocol3, ocol4 = st.columns(2)
                with ocol3:
                    orders_8wk = forecast_df.head(8)['orders_ensemble'].sum()
                    st.markdown(render_metric_card(f"${orders_8wk:,.0f}", "Next 8 Weeks", "primary"), unsafe_allow_html=True)
                with ocol4:
                    orders_12wk = forecast_df['orders_ensemble'].sum()
                    st.markdown(render_metric_card(f"${orders_12wk:,.0f}", "Next 12 Weeks", "primary"), unsafe_allow_html=True)

            with mcol2:
                st.markdown('<p style="color: #ccd6f6; font-size: 14px; margin-top: 10px;">📉 <b>Depletion Forecast</b></p>', unsafe_allow_html=True)
                dcol1, dcol2 = st.columns(2)
                with dcol1:
                    d_first = forecast_df.head(6)['depletion_ensemble'].mean()
                    d_second = forecast_df.tail(6)['depletion_ensemble'].mean()
                    d_trend = ((d_second - d_first) / d_first * 100) if d_first > 0 else 0
                    d_label = "Trending Up" if d_trend > 5 else ("Trending Down" if d_trend < -5 else "Stable")
                    d_type = "primary" if d_trend > 5 else ("danger" if d_trend < -5 else "warning")
                    st.markdown(render_metric_card(f"{d_trend:+.1f}%", d_label, d_type), unsafe_allow_html=True)
                with dcol2:
                    depl_4wk = forecast_df.head(4)['depletion_ensemble'].sum()
                    st.markdown(render_metric_card(f"{depl_4wk:,.0f} units", "Next 4 Weeks", "primary"), unsafe_allow_html=True)

                dcol3, dcol4 = st.columns(2)
                with dcol3:
                    depl_8wk = forecast_df.head(8)['depletion_ensemble'].sum()
                    st.markdown(render_metric_card(f"{depl_8wk:,.0f} units", "Next 8 Weeks", "primary"), unsafe_allow_html=True)
                with dcol4:
                    depl_12wk = forecast_df['depletion_ensemble'].sum()
                    st.markdown(render_metric_card(f"{depl_12wk:,.0f} units", "Next 12 Weeks", "primary"), unsafe_allow_html=True)

        else:
            st.info("Not enough historical data to generate forecast (need at least 4 weeks)")
    else:
        st.info("No trend data available for forecasting")

    # Charts Row 2: Top Overstocked + Top Understocked
    col1, col2 = st.columns(2)

    with col1:
        st.markdown('<p class="section-header">Top Overstocked Distributors</p>', unsafe_allow_html=True)

        overstock_df = filtered_df[filtered_df['inventory_status'] == 'Overstock'].nlargest(10, 'weeks_of_inventory')

        if not overstock_df.empty:
            fig = go.Figure(go.Bar(
                x=overstock_df['weeks_of_inventory'],
                y=overstock_df['distributor_name'],
                orientation='h',
                marker=dict(color=COLORS['warning']),
                text=overstock_df['weeks_of_inventory'].apply(lambda x: f'{x:.0f} wks'),
                textposition='outside',
                textfont=dict(color='#ccd6f6'),
                hovertemplate='%{y}<br>Weeks: %{x:.1f}<extra></extra>'
            ))

            apply_dark_theme(fig, height=350, margin=dict(l=0, r=50, t=10, b=0), yaxis={'autorange': 'reversed'})
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No overstocked distributors found")

    with col2:
        st.markdown('<p class="section-header">Top Understocked Distributors</p>', unsafe_allow_html=True)

        understock_df = filtered_df[filtered_df['inventory_status'] == 'Understock'].nsmallest(10, 'weeks_of_inventory')

        if not understock_df.empty:
            fig = go.Figure(go.Bar(
                x=understock_df['weeks_of_inventory'],
                y=understock_df['distributor_name'],
                orientation='h',
                marker=dict(color=COLORS['danger']),
                text=understock_df['weeks_of_inventory'].apply(lambda x: f'{x:.1f} wks'),
                textposition='outside',
                textfont=dict(color='#ccd6f6'),
                hovertemplate='%{y}<br>Weeks: %{x:.1f}<extra></extra>'
            ))

            apply_dark_theme(fig, height=350, margin=dict(l=0, r=50, t=10, b=0), yaxis={'autorange': 'reversed'})
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No understocked distributors found")

    # Distributor Detail Table
    st.markdown('<p class="section-header">Distributor Inventory Summary</p>', unsafe_allow_html=True)

    display_df = filtered_df[[
        'distributor_name', 'vip_codes', 'total_qty_ordered', 'total_qty_depleted',
        'order_depletion_ratio', 'weeks_of_inventory', 'inventory_status',
        'total_order_value', 'has_vip_match'
    ]].copy()

    display_df['total_order_value'] = display_df['total_order_value'].apply(lambda x: f"${x:,.0f}" if pd.notna(x) else "N/A")
    display_df['weeks_of_inventory'] = display_df['weeks_of_inventory'].apply(lambda x: f"{x:.1f}" if pd.notna(x) else "N/A")
    display_df['order_depletion_ratio'] = display_df['order_depletion_ratio'].apply(lambda x: f"{x:.2f}" if pd.notna(x) else "N/A")
    display_df['has_vip_match'] = display_df['has_vip_match'].apply(lambda x: "Yes" if x else "No")
    display_df['vip_codes'] = display_df['vip_codes'].fillna('-')
    display_df.columns = ['Distributor', 'VIP Codes', 'Qty Ordered', 'Qty Depleted', 'O/D Ratio', 'Weeks Inv', 'Status', 'Order Value', 'VIP Match']

    st.dataframe(
        display_df.sort_values('Qty Ordered', ascending=False),
        use_container_width=True,
        hide_index=True,
        height=400
    )

    # Product-Level Analysis Section
    st.markdown('<p class="section-header">Product-Level Depletion Analysis</p>', unsafe_allow_html=True)

    # Load product data for selected distributors
    selected_codes = None
    if "All Distributors" not in selected_distributors and len(selected_distributors) > 0:
        selected_codes = filtered_df['distributor_code'].tolist()

    try:
        product_df = load_product_level_data(distributor_codes=selected_codes, lookback_days=lookback_days)

        if not product_df.empty:
            # Top products by depletion
            col1, col2 = st.columns(2)

            with col1:
                st.markdown("**Top Products by Depletion Volume**")
                top_products = product_df.groupby('product_name').agg({
                    'qty_depleted': 'sum',
                    'stores_reached': 'sum',
                    'transaction_count': 'sum'
                }).nlargest(15, 'qty_depleted').reset_index()

                fig = go.Figure(go.Bar(
                    x=top_products['qty_depleted'],
                    y=top_products['product_name'],
                    orientation='h',
                    marker=dict(
                        color=top_products['qty_depleted'],
                        colorscale=[[0, COLORS['secondary']], [1, COLORS['primary']]],
                    ),
                    hovertemplate='%{y}<br>Depleted: %{x:,}<extra></extra>'
                ))

                apply_dark_theme(fig, height=400, margin=dict(l=0, r=0, t=10, b=0), yaxis={'autorange': 'reversed'})
                st.plotly_chart(fig, use_container_width=True)

            with col2:
                st.markdown("**Product Velocity Distribution**")
                velocity_counts = product_df['velocity_status'].value_counts().reset_index()
                velocity_counts.columns = ['Velocity', 'Count']

                fig = go.Figure(data=[go.Pie(
                    labels=velocity_counts['Velocity'],
                    values=velocity_counts['Count'],
                    hole=0.5,
                    marker=dict(colors=[COLORS['success'], COLORS['warning'], COLORS['info']]),
                    textinfo='label+percent',
                    textposition='outside',
                    textfont=dict(color='#ccd6f6')
                )])

                apply_dark_theme(fig, height=400, showlegend=False)
                st.plotly_chart(fig, use_container_width=True)

        else:
            st.info("No product-level data available for selected filters")
    except Exception as e:
        st.warning(f"Could not load product-level data: {e}")

    # US State Depletion Hex Map
    st.markdown('<p class="section-header">Depletion by State</p>', unsafe_allow_html=True)

    try:
        state_df = load_state_depletion_data(lookback_days=lookback_days)

        if not state_df.empty:
            # Hex tile grid positions for US states (row, col)
            # Proper hex grid layout - odd rows offset by 0.5
            # Based on standard US hex tile map template
            hex_positions = {
                # Row 0
                'AK': (0, 0), 'ME': (0, 11),
                # Row 1 (offset +0.5)
                'VT': (1, 9.5), 'NH': (1, 10.5),
                # Row 2
                'WA': (2, 1), 'MT': (2, 2), 'ND': (2, 3), 'MN': (2, 4), 'WI': (2, 5), 'MI': (2, 7), 'NY': (2, 8), 'MA': (2, 9), 'RI': (2, 10),
                # Row 3 (offset +0.5)
                'ID': (3, 1.5), 'WY': (3, 2.5), 'SD': (3, 3.5), 'IA': (3, 4.5), 'IL': (3, 5.5), 'IN': (3, 6.5), 'OH': (3, 7.5), 'PA': (3, 8.5), 'NJ': (3, 9.5), 'CT': (3, 10.5),
                # Row 4
                'OR': (4, 1), 'NV': (4, 2), 'CO': (4, 3), 'NE': (4, 4), 'MO': (4, 5), 'KY': (4, 6), 'WV': (4, 7), 'MD': (4, 8), 'DE': (4, 9), 'DC': (4, 11),
                # Row 5 (offset +0.5)
                'CA': (5, 1.5), 'AZ': (5, 2.5), 'UT': (5, 3.5), 'KS': (5, 4.5), 'AR': (5, 5.5), 'TN': (5, 6.5), 'VA': (5, 7.5), 'NC': (5, 8.5),
                # Row 6
                'NM': (6, 3), 'OK': (6, 4), 'LA': (6, 5), 'MS': (6, 6), 'AL': (6, 7), 'SC': (6, 8), 'GA': (6, 9),
                # Row 7 (offset +0.5)
                'TX': (7, 4.5), 'FL': (7, 9.5),
                # Row 8
                'HI': (8, 1),
            }

            # Create data for hex map
            hex_data = []
            max_depleted = state_df['total_depleted'].max()

            for _, row in state_df.iterrows():
                state = row['state']
                if state in hex_positions:
                    grid_row, grid_col = hex_positions[state]
                    hex_data.append({
                        'state': state,
                        'row': grid_row,
                        'col': grid_col,
                        'depleted': row['total_depleted'],
                        'doors': row['total_doors'],
                        'pods': row['total_pods']
                    })

            hex_df = pd.DataFrame(hex_data)

            # Create hex map using scatter plot with markers
            fig = go.Figure()

            # Normalize for color scale
            hex_df['color_val'] = hex_df['depleted'] / max_depleted if max_depleted > 0 else 0

            # Add hexagons as markers
            fig.add_trace(go.Scatter(
                x=hex_df['col'],
                y=-hex_df['row'],  # Negative to flip y-axis (row 0 at top)
                mode='markers+text',
                marker=dict(
                    size=55,
                    symbol='hexagon',
                    color=hex_df['depleted'],
                    colorscale=[
                        [0, '#1a1a2e'],
                        [0.2, '#0f3460'],
                        [0.5, '#00a3cc'],
                        [0.8, '#00d4aa'],
                        [1, '#64ffda']
                    ],
                    colorbar=dict(
                        title=dict(text='Units', font=dict(color='#ccd6f6', size=12)),
                        tickfont=dict(color='#8892b0', size=10),
                        thickness=15,
                        len=0.7
                    ),
                    line=dict(color='rgba(255,255,255,0.3)', width=1)
                ),
                text=hex_df.apply(lambda r: f"<b>{r['state']}</b><br>{r['depleted']/1000:.1f}K", axis=1),
                textposition='middle center',
                textfont=dict(color='white', size=10),
                hovertemplate='<b>%{customdata[0]}</b><br>' +
                              'Depleted: %{customdata[1]:,.0f}<br>' +
                              'Doors: %{customdata[2]:,.0f}<br>' +
                              'PODs: %{customdata[3]:,.0f}<extra></extra>',
                customdata=hex_df[['state', 'depleted', 'doors', 'pods']].values
            ))

            fig.update_layout(
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)',
                margin=dict(l=20, r=20, t=20, b=20),
                height=400,
                xaxis=dict(visible=False, range=[-1, 14]),
                yaxis=dict(visible=False, scaleanchor='x', scaleratio=1),
                showlegend=False
            )

            st.plotly_chart(fig, use_container_width=True)

            # State coverage stats in a row
            total_states = len(state_df)
            total_depleted = state_df['total_depleted'].sum()
            total_doors = state_df['total_doors'].sum()
            total_pods = state_df['total_pods'].sum()
            top_state = state_df.iloc[0]['state'] if len(state_df) > 0 else 'N/A'
            top_state_pct = (state_df.iloc[0]['total_depleted'] / total_depleted * 100) if total_depleted > 0 else 0

            metric_cols = st.columns(4)
            with metric_cols[0]:
                st.markdown(f"""
                <div class="metric-card">
                    <p class="metric-value">{total_states}</p>
                    <p class="metric-label">States with Depletion</p>
                </div>
                """, unsafe_allow_html=True)
            with metric_cols[1]:
                st.markdown(f"""
                <div class="metric-card">
                    <p class="metric-value">{total_doors:,.0f}</p>
                    <p class="metric-label">Total Doors (Active)</p>
                </div>
                """, unsafe_allow_html=True)
            with metric_cols[2]:
                st.markdown(f"""
                <div class="metric-card">
                    <p class="metric-value">{total_pods:,.0f}</p>
                    <p class="metric-label">Total PODs</p>
                </div>
                """, unsafe_allow_html=True)
            with metric_cols[3]:
                st.markdown(f"""
                <div class="metric-card">
                    <p class="metric-value">{top_state}</p>
                    <p class="metric-label">Top State ({top_state_pct:.1f}%)</p>
                </div>
                """, unsafe_allow_html=True)

            # Full-width state table
            st.markdown("**Top States by Depletion**")
            state_display = state_df[['state', 'distributor_count', 'total_depleted', 'total_doors', 'total_pods', 'avg_pods_per_dist', 'weekly_rate']].head(15).copy()
            state_display['total_depleted'] = state_display['total_depleted'].apply(lambda x: f"{x:,.0f}")
            state_display['weekly_rate'] = state_display['weekly_rate'].apply(lambda x: f"{x:,.0f}")
            state_display['total_doors'] = state_display['total_doors'].apply(lambda x: f"{x:,.0f}")
            state_display['total_pods'] = state_display['total_pods'].apply(lambda x: f"{x:,.0f}")
            state_display['avg_pods_per_dist'] = state_display['avg_pods_per_dist'].apply(lambda x: f"{x:,.1f}")
            state_display.columns = ['State', 'Distributors', 'Total Depleted', 'Doors', 'PODs', 'SKUs/Door', 'Weekly Rate']

            st.dataframe(
                state_display,
                use_container_width=True,
                hide_index=True,
                height=400
            )
        else:
            st.info("No state-level depletion data available")
    except Exception as e:
        st.warning(f"Could not load state depletion data: {e}")

    # Footer
    st.markdown(f"""
    <div style="text-align: center; color: #8892b0; margin-top: 48px; padding: 24px; border-top: 1px solid rgba(255,255,255,0.1);">
        <p style="margin: 0;">Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} UTC</p>
        <p style="margin: 4px 0 0 0; font-size: 12px;">Data refreshes every 5 minutes | Lookback: {lookback_days} days</p>
    </div>
    """, unsafe_allow_html=True)


if __name__ == "__main__":
    main()
