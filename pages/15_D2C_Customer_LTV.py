"""
D2C Customer LTV Dashboard
WooCommerce (2023-2025) + Shopify (2025+) unified customer lifetime value analytics.
Cohort analysis, retention, platform migration tracking.
"""

import streamlit as st
import pandas as pd
from google.cloud import bigquery
from datetime import datetime, date
import plotly.graph_objects as go
import plotly.express as px
import numpy as np

from nowadays_ui import editorial_plotly, inject_editorial_style

inject_editorial_style()


COLORS = {
    'primary': '#2D2926',
    'secondary': '#074A7A',
    'teal': '#3F634E',
    'gold': '#8a6b00',
    'blue': '#074A7A',
    'purple': '#074A7A',
    'woo': '#96588a',
    'shopify': '#3F634E',
}


def apply_dark_theme(fig, height=350, **kwargs):
    return editorial_plotly(fig, height=height, **kwargs)


@st.cache_resource
def get_bq_client():
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


def _run_query(query):
    """Run BQ query and force all numeric-like columns to native float/int."""
    client = get_bq_client()
    df = client.query(query).to_dataframe()
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            continue
        try:
            df[col] = pd.to_numeric(df[col])
        except (ValueError, TypeError, ArithmeticError):
            pass
    return df


def _f(val):
    """Force a value to native float (handles Decimal, numpy, etc)."""
    return float(val) if val is not None else 0.0


def _platform_cols(platform, date_start=None, date_end=None):
    """Return (revenue_col, orders_col, where_clause) for the selected platform + optional date range."""
    conditions = []
    if platform == "WooCommerce":
        rev, ord_ = "woo_revenue", "woo_orders"
        conditions.append("woo_orders > 0")
    elif platform == "Shopify":
        rev, ord_ = "shopify_revenue", "shopify_orders"
        conditions.append("shopify_orders > 0")
    else:
        rev, ord_ = "lifetime_revenue", "lifetime_orders"

    if date_start:
        conditions.append(f"DATE(first_order_date) >= '{date_start}'")
    if date_end:
        conditions.append(f"DATE(first_order_date) <= '{date_end}'")

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    return rev, ord_, where


def compute_mature_ltv(cohort_df, horizon_months, as_of_date):
    """
    Customer-weighted LTV over cohorts that have fully baked through `horizon_months`.

    A cohort qualifies iff parse(cohort, 'YYYY-MM') + horizon_months
    <= DATE_TRUNC(as_of_date, MONTH). The comparison is <=, so a cohort
    whose first month is exactly `horizon_months` calendar months before the
    current month qualifies.

    The formula is the customer-weighted-average identity (Joe's "SUMPRODUCT"):
        ltv = SUM(cohort_revenue_through_M-1) / SUM(cohort_size)

    Args:
        cohort_df: DataFrame from load_cohort_data() with columns
            ['cohort', 'months_since_first', 'customers', 'revenue', ...].
            `cohort` is a 'YYYY-MM' string.
        horizon_months: 6 or 12.
        as_of_date: datetime.date — the reference date for the maturity cutoff.

    Returns:
        dict with keys:
            ltv (float), qualifying_cohorts (int),
            earliest_cohort (str | None), latest_cohort (str | None),
            total_customers (int), total_revenue (float)
    """
    empty = {
        'ltv': 0.0,
        'qualifying_cohorts': 0,
        'earliest_cohort': None,
        'latest_cohort': None,
        'total_customers': 0,
        'total_revenue': 0.0,
    }
    if cohort_df is None or cohort_df.empty:
        return empty

    df = cohort_df.copy()
    df['cohort_period'] = pd.PeriodIndex(df['cohort'], freq='M')
    as_of_period = pd.Period(pd.Timestamp(as_of_date), freq='M')
    cutoff_period = as_of_period - horizon_months

    qualifying_cohorts = df[df['cohort_period'] <= cutoff_period]['cohort_period'].unique()
    if len(qualifying_cohorts) == 0:
        return empty

    qual_mask = df['cohort_period'].isin(qualifying_cohorts)
    # Numerator: revenue from qualifying cohorts in months [0, horizon_months - 1]
    rev_mask = qual_mask & df['months_since_first'].between(0, horizon_months - 1)
    total_revenue = float(df.loc[rev_mask, 'revenue'].sum())

    # Denominator: M0 customer counts across qualifying cohorts
    m0_mask = qual_mask & (df['months_since_first'] == 0)
    total_customers = int(df.loc[m0_mask, 'customers'].sum())

    if total_customers == 0:
        return empty

    sorted_cohorts = sorted(str(p) for p in qualifying_cohorts)
    return {
        'ltv': total_revenue / total_customers,
        'qualifying_cohorts': len(qualifying_cohorts),
        'earliest_cohort': sorted_cohorts[0],
        'latest_cohort': sorted_cohorts[-1],
        'total_customers': total_customers,
        'total_revenue': total_revenue,
    }


def compute_cumulative_matrix(cohort_df, as_of_date):
    """
    Cohort x months_since_first matrix of cumulative $ per customer.

    Cells where parse(cohort, 'YYYY-MM') + m months > (month-start of as_of_date - 1 month)
    are set to NaN (unbaked), producing a triangular heatmap.

    Args:
        cohort_df: DataFrame from load_cohort_data() with columns
            ['cohort', 'months_since_first', 'customers', 'revenue', ...].
        as_of_date: datetime.date — reference date for the bake line.

    Returns:
        pd.DataFrame indexed by cohort label ('YYYY-MM' string, oldest first),
        columns are months_since_first (int, 0..max), values are cumulative
        $/customer (float) or NaN for unbaked cells.
        Returns empty DataFrame if cohort_df is empty.
    """
    if cohort_df is None or cohort_df.empty:
        return pd.DataFrame()

    df = cohort_df.copy()

    # Cohort sizes (M0 customer counts)
    sizes = (
        df[df['months_since_first'] == 0]
        .set_index('cohort')['customers']
        .astype(float)
    )
    if sizes.empty:
        return pd.DataFrame()

    # Per-cell revenue pivot, fill missing M/c pairs with 0 so cumsum is well-defined
    rev_pivot = (
        df.pivot_table(
            index='cohort',
            columns='months_since_first',
            values='revenue',
            aggfunc='sum',
            fill_value=0,
        )
        .astype(float)
    )

    # Cumulative revenue across months_since_first, then divide by cohort size
    cum_rev = rev_pivot.cumsum(axis=1)
    cum_per_customer = cum_rev.div(sizes, axis=0)

    # Sort oldest -> newest by cohort label (YYYY-MM strings sort correctly)
    cum_per_customer = cum_per_customer.sort_index()

    # Bake-line masking: cell (c, m) is baked iff (cohort c + m months) is a
    # month that has fully elapsed, i.e. <= the last complete month before as_of_date.
    as_of_period = pd.Period(pd.Timestamp(as_of_date), freq='M')
    last_complete_month = as_of_period - 1  # previous full month
    cohort_periods = pd.PeriodIndex(cum_per_customer.index, freq='M')
    for m in cum_per_customer.columns:
        cell_periods = cohort_periods + int(m)
        unbaked = cell_periods > last_complete_month
        if unbaked.any():
            cum_per_customer.loc[unbaked, m] = np.nan

    return cum_per_customer


# --- Data Loaders ---

@st.cache_data(ttl=600)
def load_ltv_summary(platform="All", date_start=None, date_end=None):
    rev, ord_, where = _platform_cols(platform, date_start, date_end)
    return _run_query(f"""
    SELECT
        COUNT(*) AS total_customers,
        ROUND(SUM({rev}), 2) AS total_revenue,
        ROUND(AVG({rev}), 2) AS avg_ltv,
        ROUND(APPROX_QUANTILES({rev}, 100)[OFFSET(50)], 2) AS median_ltv,
        ROUND(AVG({ord_}), 1) AS avg_orders,
        COUNTIF({ord_} > 1) AS repeat_customers,
        COUNTIF(woo_orders > 0 AND shopify_orders > 0) AS cross_platform,
        ROUND(SUM(woo_revenue), 2) AS woo_revenue,
        ROUND(SUM(shopify_revenue), 2) AS shopify_revenue,
        COUNTIF(woo_orders > 0) AS woo_customers,
        COUNTIF(shopify_orders > 0) AS shopify_customers,
        ROUND(AVG(customer_lifespan_days), 0) AS avg_lifespan_days,
        COUNTIF(refunded_orders > 0) AS customers_with_refunds
    FROM `artful-logic-475116-p1.marts.vw_d2c_customer_ltv`
    {where}
    """)


@st.cache_data(ttl=600)
def load_ltv_distribution(platform="All", date_start=None, date_end=None):
    rev, _, where = _platform_cols(platform, date_start, date_end)
    return _run_query(f"""
    SELECT
        CASE
            WHEN {rev} < 25 THEN '$0-25'
            WHEN {rev} < 50 THEN '$25-50'
            WHEN {rev} < 100 THEN '$50-100'
            WHEN {rev} < 200 THEN '$100-200'
            WHEN {rev} < 500 THEN '$200-500'
            WHEN {rev} < 1000 THEN '$500-1K'
            ELSE '$1K+'
        END AS ltv_bucket,
        CASE
            WHEN {rev} < 25 THEN 1
            WHEN {rev} < 50 THEN 2
            WHEN {rev} < 100 THEN 3
            WHEN {rev} < 200 THEN 4
            WHEN {rev} < 500 THEN 5
            WHEN {rev} < 1000 THEN 6
            ELSE 7
        END AS bucket_order,
        COUNT(*) AS customers,
        ROUND(SUM({rev}), 0) AS bucket_revenue
    FROM `artful-logic-475116-p1.marts.vw_d2c_customer_ltv`
    {where}
    GROUP BY 1, 2
    ORDER BY 2
    """)


@st.cache_data(ttl=600)
def load_monthly_revenue(date_start=None, date_end=None):
    woo_date_filter = ""
    shopify_date_filter = ""
    if date_start:
        woo_date_filter += f" AND DATE(date_created) >= '{date_start}'"
        shopify_date_filter += f" AND DATE(created_at) >= '{date_start}'"
    if date_end:
        woo_date_filter += f" AND DATE(date_created) <= '{date_end}'"
        shopify_date_filter += f" AND DATE(created_at) <= '{date_end}'"
    return _run_query(f"""
    WITH woo AS (
        SELECT
            DATE_TRUNC(TIMESTAMP(date_created), MONTH) AS month,
            COUNT(*) AS orders,
            SUM(CAST(total AS NUMERIC)) AS revenue,
            'WooCommerce' AS platform
        FROM `artful-logic-475116-p1.raw_woocommerce.orders`
        WHERE status IN ('completed', 'refunded'){woo_date_filter}
        GROUP BY 1
    ),
    shopify AS (
        SELECT
            DATE_TRUNC(created_at, MONTH) AS month,
            COUNT(*) AS orders,
            SUM(total_price) AS revenue,
            'Shopify' AS platform
        FROM `artful-logic-475116-p1.raw_shopify.orders`
        WHERE financial_status IN ('paid', 'partially_refunded', 'refunded')
            AND cancelled_at IS NULL{shopify_date_filter}
        GROUP BY 1
    )
    SELECT * FROM woo
    UNION ALL
    SELECT * FROM shopify
    ORDER BY month, platform
    """)


@st.cache_data(ttl=600)
def load_cohort_data(platform="All", date_start=None, date_end=None):
    cohort_filter = ""
    if date_start:
        cohort_filter += f" AND DATE(cohort_month) >= '{date_start}'"
    if date_end:
        cohort_filter += f" AND DATE(cohort_month) <= '{date_end}'"
    if platform == "All":
        return _run_query(f"""
        SELECT
            FORMAT_TIMESTAMP('%Y-%m', cohort_month) AS cohort,
            months_since_first,
            customers,
            orders,
            revenue,
            revenue_per_customer
        FROM `artful-logic-475116-p1.marts.vw_d2c_cohort_analysis`
        WHERE months_since_first <= 18{cohort_filter}
        ORDER BY cohort, months_since_first
        """)

    # Platform-specific: inline query with source filter since the view doesn't have it
    source = 'woocommerce' if platform == "WooCommerce" else 'shopify'
    return _run_query(f"""
    WITH woo_orders AS (
      SELECT order_id, LOWER(TRIM(billing_email)) AS email,
        TIMESTAMP(date_created) AS order_date, CAST(total AS NUMERIC) AS total,
        'woocommerce' AS source
      FROM `artful-logic-475116-p1.raw_woocommerce.orders`
      WHERE status IN ('completed', 'refunded')
        AND billing_email IS NOT NULL AND TRIM(billing_email) != ''
    ),
    shopify_orders AS (
      SELECT id AS order_id, LOWER(TRIM(email)) AS email,
        created_at AS order_date, total_price AS total,
        'shopify' AS source
      FROM `artful-logic-475116-p1.raw_shopify.orders`
      WHERE financial_status IN ('paid', 'partially_refunded', 'refunded')
        AND cancelled_at IS NULL
        AND email IS NOT NULL AND TRIM(email) != ''
    ),
    overlap_dupes AS (
      SELECT DISTINCT w.order_id AS woo_order_id
      FROM woo_orders w
      INNER JOIN shopify_orders s
        ON w.email = s.email
        AND DATE(w.order_date) = DATE(s.order_date)
        AND ABS(w.total - s.total) < 1.00
      WHERE w.order_date >= '2025-06-01'
        AND w.order_date < '2025-10-01'
    ),
    all_orders AS (
      SELECT * FROM woo_orders
      WHERE order_id NOT IN (SELECT woo_order_id FROM overlap_dupes)
      UNION ALL
      SELECT * FROM shopify_orders
    ),
    filtered AS (
      SELECT * FROM all_orders WHERE source = '{source}'
    ),
    customer_first AS (
      SELECT email, DATE_TRUNC(MIN(order_date), MONTH) AS cohort_month
      FROM filtered GROUP BY email
    ),
    order_with_cohort AS (
      SELECT o.*, cf.cohort_month,
        DATE_DIFF(DATE_TRUNC(DATE(o.order_date), MONTH), DATE(cf.cohort_month), MONTH) AS months_since_first
      FROM filtered o JOIN customer_first cf USING (email)
    )
    SELECT
      FORMAT_TIMESTAMP('%Y-%m', cohort_month) AS cohort,
      months_since_first,
      COUNT(DISTINCT email) AS customers,
      COUNT(*) AS orders,
      ROUND(SUM(total), 2) AS revenue,
      ROUND(SUM(total) / COUNT(DISTINCT email), 2) AS revenue_per_customer
    FROM order_with_cohort
    WHERE months_since_first <= 18{cohort_filter}
    GROUP BY 1, 2
    ORDER BY 1, 2
    """)


@st.cache_data(ttl=600)
def load_repeat_rate_trend(platform="All", date_start=None, date_end=None):
    _, ord_, where = _platform_cols(platform, date_start, date_end)
    return _run_query(f"""
    WITH customer_cohorts AS (
        SELECT
            email,
            DATE_TRUNC(first_order_date, QUARTER) AS acquisition_quarter,
            {ord_} AS platform_orders
        FROM `artful-logic-475116-p1.marts.vw_d2c_customer_ltv`
        {where}
    )
    SELECT
        FORMAT_TIMESTAMP('%Y-Q%Q', acquisition_quarter) AS quarter,
        COUNT(*) AS total_customers,
        COUNTIF(platform_orders > 1) AS repeat_customers,
        ROUND(COUNTIF(platform_orders > 1) / COUNT(*) * 100, 1) AS repeat_rate,
        ROUND(AVG(platform_orders), 2) AS avg_orders
    FROM customer_cohorts
    GROUP BY 1, acquisition_quarter
    ORDER BY acquisition_quarter
    """)


@st.cache_data(ttl=600)
def load_top_customers(platform="All", date_start=None, date_end=None):
    rev, ord_, where = _platform_cols(platform, date_start, date_end)
    return _run_query(f"""
    SELECT
        email,
        {ord_} AS lifetime_orders,
        ROUND({rev}, 2) AS lifetime_revenue,
        DATE(first_order) AS first_order,
        DATE(last_order) AS last_order,
        woo_orders,
        shopify_orders,
        refunded_orders
    FROM `artful-logic-475116-p1.marts.vw_d2c_customer_ltv`
    {where}
    ORDER BY {rev} DESC
    LIMIT 50
    """)


# --- Render Helpers ---

def render_metric(value, label, style="primary", sub=None):
    cls = {
        'primary': 'metric-value',
        'teal': 'metric-value-teal',
        'gold': 'metric-value-gold',
        'blue': 'metric-value-blue',
    }.get(style, 'metric-value')
    sub_html = f'<div class="metric-sub">{sub}</div>' if sub else ''
    return f"""
    <div class="metric-card">
        <div class="{cls}">{value}</div>
        <div class="metric-label">{label}</div>
        {sub_html}
    </div>
    """


def main():
    st.markdown("""
    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;">
        <div>
            <h1 class="dashboard-header">D2C Customer LTV</h1>
            <p class="dashboard-subtitle">WooCommerce (2023-2025) + Shopify (2025+) &mdash; Unified Customer Lifetime Value</p>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # --- Filters ---
    filter_cols = st.columns([2, 1, 1, 1])
    with filter_cols[0]:
        platform = st.radio(
            "Platform",
            ["Shopify", "All", "WooCommerce"],
            horizontal=True,
            label_visibility="collapsed",
        )
    with filter_cols[1]:
        date_start = st.date_input("From", value=None, min_value=date(2023, 1, 1), max_value=date.today(), key="ltv_date_start")
    with filter_cols[2]:
        date_end = st.date_input("To", value=None, min_value=date(2023, 1, 1), max_value=date.today(), key="ltv_date_end")
    with filter_cols[3]:
        if st.button("Clear dates", use_container_width=True):
            st.session_state["ltv_date_start"] = None
            st.session_state["ltv_date_end"] = None
            st.rerun()

    ds = str(date_start) if date_start else None
    de = str(date_end) if date_end else None

    st.markdown("<br>", unsafe_allow_html=True)

    try:
        summary = load_ltv_summary(platform, ds, de)
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return

    if summary.empty:
        st.warning("No LTV data available. Ensure the BQ views exist.")
        return

    s = summary.iloc[0]
    total_revenue = _f(s['total_revenue'])
    total_customers = _f(s['total_customers'])
    repeat_customers = _f(s['repeat_customers'])
    cross_platform = _f(s['cross_platform'])
    woo_revenue = _f(s['woo_revenue'])
    shopify_revenue = _f(s['shopify_revenue'])
    woo_customers = _f(s['woo_customers'])
    shopify_customers = _f(s['shopify_customers'])
    repeat_rate = (repeat_customers / total_customers * 100) if total_customers else 0

    # Load cohort data once — feeds both the new mature-LTV KPI cards
    # and the Cohort Retention tab. Cached at the loader level.
    try:
        cohort_df = load_cohort_data(platform, ds, de)
    except Exception as e:
        st.error(f"Error loading cohort data: {e}")
        cohort_df = pd.DataFrame()

    today = date.today()
    ltv_6mo = compute_mature_ltv(cohort_df, 6, today)
    ltv_12mo = compute_mature_ltv(cohort_df, 12, today)

    def _fmt_ltv_sub(r):
        if r['qualifying_cohorts'] == 0:
            return "no mature cohorts"
        return f"{r['qualifying_cohorts']} cohorts · {r['earliest_cohort']} → {r['latest_cohort']}"

    # --- KPI Row ---
    avg_ltv = total_revenue / total_customers if total_customers else 0
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    with c1:
        rev_display = f"${total_revenue/1e6:.1f}M" if total_revenue >= 1e6 else f"${total_revenue/1e3:.0f}K"
        st.markdown(render_metric(rev_display, "Lifetime Revenue", sub=f"${avg_ltv:.0f} avg per customer"), unsafe_allow_html=True)
    with c2:
        st.markdown(render_metric(f"{total_customers:,.0f}", "Unique Customers", sub=f"{repeat_customers:,.0f} repeat ({repeat_rate:.0f}%)"), unsafe_allow_html=True)
    with c3:
        st.markdown(render_metric(f"${ltv_6mo['ltv']:.0f}", "6-Month LTV", sub=_fmt_ltv_sub(ltv_6mo)), unsafe_allow_html=True)
    with c4:
        st.markdown(render_metric(f"${ltv_12mo['ltv']:.0f}", "12-Month LTV", sub=_fmt_ltv_sub(ltv_12mo)), unsafe_allow_html=True)
    with c5:
        st.markdown(render_metric(f"{repeat_rate:.1f}%", "Repeat Rate", sub=f"{repeat_customers:,.0f} of {total_customers:,.0f}"), unsafe_allow_html=True)
    with c6:
        if platform == "All":
            st.markdown(render_metric(f"{cross_platform:,.0f}", "Cross-Platform", sub="ordered on Woo + Shopify"), unsafe_allow_html=True)
        else:
            lifespan = _f(s['avg_lifespan_days'])
            st.markdown(render_metric(f"{lifespan:.0f}d", "Avg Lifespan", sub="days between orders"), unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # --- Platform Insight Banner ---
    if platform == "All":
        woo_pct = woo_revenue / total_revenue * 100 if total_revenue else 0
        shopify_pct = 100 - woo_pct
        st.markdown(f"""
        <div class="insight-banner">
            <strong>Platform Split:</strong>
            <span style="color: {COLORS['woo']}; margin-left: 12px;">WooCommerce</span>
            ${woo_revenue/1e6:.1f}M ({woo_pct:.0f}%) &bull; {woo_customers:,.0f} customers
            &nbsp;&nbsp;|&nbsp;&nbsp;
            <span style="color: {COLORS['shopify']};">Shopify</span>
            ${shopify_revenue/1e6:.1f}M ({shopify_pct:.0f}%) &bull; {shopify_customers:,.0f} customers
        </div>
        """, unsafe_allow_html=True)
    else:
        color = COLORS['woo'] if platform == "WooCommerce" else COLORS['shopify']
        st.markdown(f"""
        <div class="insight-banner">
            <strong style="color: {color};">Showing: {platform} only</strong>
            &nbsp;&mdash;&nbsp;{total_customers:,.0f} customers &bull; ${total_revenue/1e6:.1f}M revenue &bull; {cross_platform:,.0f} also on {'Shopify' if platform == 'WooCommerce' else 'WooCommerce'}
        </div>
        """, unsafe_allow_html=True)

    # --- Tabs ---
    tab1, tab2, tab3, tab4 = st.tabs(["Revenue Timeline", "LTV Distribution", "Cohort Retention", "Top Customers"])

    # === Tab 1: Revenue Timeline ===
    with tab1:
        st.markdown('<p class="section-header">Monthly Revenue by Platform</p>', unsafe_allow_html=True)

        try:
            monthly = load_monthly_revenue(ds, de)
        except Exception as e:
            st.error(f"Error: {e}")
            monthly = pd.DataFrame()

        if not monthly.empty:
            monthly['month'] = pd.to_datetime(monthly['month'])

            # Filter by platform if needed
            if platform != "All":
                monthly = monthly[monthly['platform'] == platform].copy()

            fig = go.Figure()
            platforms_to_show = [platform] if platform != "All" else ['WooCommerce', 'Shopify']
            for plat in platforms_to_show:
                color = COLORS['woo'] if plat == 'WooCommerce' else COLORS['shopify']
                df = monthly[monthly['platform'] == plat]
                fig.add_trace(go.Bar(
                    x=df['month'],
                    y=df['revenue'].astype(float),
                    name=plat,
                    marker_color=color,
                    hovertemplate='%{x|%b %Y}<br>$%{y:,.0f}<extra>' + plat + '</extra>'
                ))

            apply_dark_theme(fig, height=400,
                barmode='stack',
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font=dict(color='#6B6560')),
                xaxis={'title': ''},
                yaxis={'title': 'Revenue ($)'}
            )
            st.plotly_chart(fig, use_container_width=True)

            # Orders trend
            st.markdown('<p class="section-header">Monthly Order Volume</p>', unsafe_allow_html=True)
            fig2 = go.Figure()
            for plat in platforms_to_show:
                color = COLORS['woo'] if plat == 'WooCommerce' else COLORS['shopify']
                df = monthly[monthly['platform'] == plat]
                fig2.add_trace(go.Scatter(
                    x=df['month'],
                    y=df['orders'].astype(float),
                    name=plat,
                    mode='lines+markers',
                    line=dict(color=color, width=2),
                    marker=dict(size=5),
                    hovertemplate='%{x|%b %Y}<br>%{y:,.0f} orders<extra>' + plat + '</extra>'
                ))

            apply_dark_theme(fig2, height=300,
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font=dict(color='#6B6560')),
                yaxis={'title': 'Orders'}
            )
            st.plotly_chart(fig2, use_container_width=True)

    # === Tab 2: LTV Distribution ===
    with tab2:
        st.markdown('<p class="section-header">Customer LTV Distribution</p>', unsafe_allow_html=True)

        try:
            dist = load_ltv_distribution(platform, ds, de)
        except Exception as e:
            st.error(f"Error: {e}")
            dist = pd.DataFrame()

        if not dist.empty:
            col1, col2 = st.columns(2)

            with col1:
                fig = go.Figure(go.Bar(
                    x=dist['ltv_bucket'],
                    y=dist['customers'].astype(float),
                    marker=dict(
                        color=dist['bucket_order'].astype(float),
                        colorscale=[[0, COLORS['blue']], [0.5, COLORS['primary']], [1, COLORS['secondary']]],
                    ),
                    text=dist['customers'].apply(lambda x: f'{_f(x):,.0f}'),
                    textposition='outside',
                    textfont=dict(color='#2D2926', size=11),
                    hovertemplate='%{x}<br>%{y:,.0f} customers<extra></extra>'
                ))
                apply_dark_theme(fig, height=350, yaxis={'title': 'Customers'})
                fig.update_layout(title=dict(text='Customers by LTV Bucket', font=dict(color='#6B6560', size=14)))
                st.plotly_chart(fig, use_container_width=True)

            with col2:
                fig2 = go.Figure(go.Bar(
                    x=dist['ltv_bucket'],
                    y=dist['bucket_revenue'].astype(float),
                    marker=dict(
                        color=dist['bucket_order'].astype(float),
                        colorscale=[[0, '#C4D6E5'], [0.5, '#074A7A'], [1, '#2D2926']],
                    ),
                    text=dist['bucket_revenue'].apply(lambda x: f'${_f(x)/1000:.0f}K'),
                    textposition='outside',
                    textfont=dict(color='#2D2926', size=11),
                    hovertemplate='%{x}<br>$%{y:,.0f} revenue<extra></extra>'
                ))
                apply_dark_theme(fig2, height=350, yaxis={'title': 'Revenue ($)'})
                fig2.update_layout(title=dict(text='Revenue by LTV Bucket', font=dict(color='#6B6560', size=14)))
                st.plotly_chart(fig2, use_container_width=True)

            # Key insight
            top_bucket = dist[dist['ltv_bucket'] == '$1K+']
            if not top_bucket.empty:
                top_cust = _f(top_bucket['customers'].iloc[0])
                top_rev = _f(top_bucket['bucket_revenue'].iloc[0])
                total_cust = _f(dist['customers'].sum())
                total_rev = _f(dist['bucket_revenue'].sum())
                top_pct = top_cust / total_cust * 100 if total_cust else 0
                rev_pct = top_rev / total_rev * 100 if total_rev else 0
                st.markdown(f"""
                <div class="insight-banner">
                    <strong style="color: #074A7A;">$1K+ Customers:</strong>
                    {top_cust:,.0f} customers ({top_pct:.1f}%) drive ${top_rev/1e6:.1f}M ({rev_pct:.0f}%) of total revenue
                </div>
                """, unsafe_allow_html=True)

        # Repeat rate by acquisition cohort
        st.markdown('<p class="section-header">Repeat Rate by Acquisition Quarter</p>', unsafe_allow_html=True)
        try:
            repeat_df = load_repeat_rate_trend(platform, ds, de)
        except Exception as e:
            st.error(f"Error: {e}")
            repeat_df = pd.DataFrame()

        if not repeat_df.empty:
            fig3 = go.Figure()
            fig3.add_trace(go.Bar(
                x=repeat_df['quarter'],
                y=repeat_df['total_customers'].astype(float),
                name='Total Customers',
                marker_color='rgba(116, 185, 255, 0.3)',
                yaxis='y2',
                hovertemplate='%{x}<br>%{y:,.0f} customers<extra></extra>'
            ))
            fig3.add_trace(go.Scatter(
                x=repeat_df['quarter'],
                y=repeat_df['repeat_rate'].astype(float),
                name='Repeat Rate %',
                mode='lines+markers+text',
                line=dict(color=COLORS['primary'], width=3),
                marker=dict(size=8),
                text=repeat_df['repeat_rate'].apply(lambda x: f'{_f(x):.0f}%'),
                textposition='top center',
                textfont=dict(color='#2D2926', size=10),
                hovertemplate='%{x}<br>%{y:.1f}% repeat<extra></extra>'
            ))

            apply_dark_theme(fig3, height=350,
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font=dict(color='#6B6560')),
                yaxis={'title': 'Repeat Rate (%)', 'range': [0, 100]},
            )
            fig3.update_layout(
                yaxis2=dict(
                    overlaying='y', side='right', showgrid=False,
                    title=dict(text='Customers', font=dict(color='#6B6560')),
                    tickfont=dict(color='#6B6560')
                )
            )
            st.plotly_chart(fig3, use_container_width=True)

    # === Tab 3: Cohort Retention ===
    with tab3:
        st.markdown('<p class="section-header">Cohort Retention Heatmap</p>', unsafe_allow_html=True)

        # cohort_df was loaded above for the KPI row — reuse it here

        if not cohort_df.empty:
            # Build retention matrix
            pivot = cohort_df.pivot_table(
                index='cohort',
                columns='months_since_first',
                values='customers',
                aggfunc='sum'
            ).fillna(0).astype(float)

            if 0 in pivot.columns:
                month_0 = pivot[0].replace(0, np.nan)
                retention_pct = pivot.div(month_0, axis=0) * 100
                retention_pct = retention_pct.fillna(0)

                # Only show cohorts with meaningful M0
                retention_pct = retention_pct[pivot[0] > 50]

                if not retention_pct.empty:
                    z_vals = retention_pct.values
                    z_text = np.where(z_vals > 0, np.char.add(np.round(z_vals, 1).astype(str), '%'), '')

                    fig = go.Figure(data=go.Heatmap(
                        z=z_vals,
                        x=[f'M{i}' for i in retention_pct.columns],
                        y=retention_pct.index.tolist(),
                        colorscale=[
                            [0, '#F5F0EB'],
                            [0.05, '#E8DFD6'],
                            [0.15, '#C4D6E5'],
                            [0.3, '#074A7A'],
                            [0.5, '#3F634E'],
                            [1.0, '#2D2926']
                        ],
                        text=z_text,
                        texttemplate='%{text}',
                        textfont=dict(size=9, color='#2D2926'),
                        hovertemplate='Cohort: %{y}<br>Month: %{x}<br>Retention: %{z:.1f}%<extra></extra>',
                        colorbar=dict(
                            title=dict(text='Retention %', font=dict(color='#6B6560')),
                            tickfont=dict(color='#6B6560')
                        )
                    ))

                    apply_dark_theme(fig, height=max(400, len(retention_pct) * 22),
                        xaxis={'title': 'Months Since First Purchase', 'side': 'bottom'},
                        yaxis={'title': '', 'autorange': 'reversed'},
                        margin=dict(l=80, r=20, t=20, b=60)
                    )
                    st.plotly_chart(fig, use_container_width=True)

            # --- Cumulative $ / customer matrix (Joe's ask, 2026-04-07) ---
            st.markdown('<p class="section-header">Cumulative Revenue per Customer by Cohort</p>', unsafe_allow_html=True)
            st.markdown(
                '<div class="insight-banner" style="border-left-color: #074A7A;">'
                '<strong style="color: #074A7A;">Mature-cohort view:</strong> '
                'cells show cumulative $/customer from M0 through M_n. '
                'Blank cells past the diagonal are cohorts that have not yet baked through that month — '
                'comparing them to fully-elapsed cohorts would understate LTV.'
                '</div>',
                unsafe_allow_html=True,
            )

            cum_matrix = compute_cumulative_matrix(cohort_df, date.today())
            # Drop tiny cohorts (<50 customers at M0) to match retention heatmap's noise floor
            if not cum_matrix.empty and 0 in pivot.columns:
                cum_matrix = cum_matrix.loc[cum_matrix.index.isin(pivot[pivot[0] > 50].index)]

            if not cum_matrix.empty:
                z_vals = cum_matrix.values.astype(float)
                # Format cells: $X for <1000, $X.XK for >=1000
                def _fmt_cell(v):
                    if pd.isna(v):
                        return ''
                    if v >= 1000:
                        return f'${v/1000:.1f}K'
                    return f'${v:.0f}'
                z_text = np.array([[_fmt_cell(v) for v in row] for row in z_vals])

                fig_cum = go.Figure(data=go.Heatmap(
                    z=z_vals,
                    x=[f'M{i}' for i in cum_matrix.columns],
                    y=cum_matrix.index.tolist(),
                    colorscale=[
                        [0, '#0f0f1a'],
                        [0.05, '#1a1a3e'],
                        [0.15, '#2a2a6a'],
                        [0.3, '#667eea'],
                        [0.5, '#f093fb'],
                        [1.0, '#f5576c'],
                    ],
                    text=z_text,
                    texttemplate='%{text}',
                    textfont=dict(size=9, color='#2D2926'),
                    hovertemplate='Cohort: %{y}<br>Month: %{x}<br>Cumulative $/customer: $%{z:,.2f}<extra></extra>',
                    hoverongaps=False,
                    colorbar=dict(
                        title=dict(text='$ / customer', font=dict(color='#6B6560')),
                        tickfont=dict(color='#6B6560'),
                    ),
                ))

                apply_dark_theme(
                    fig_cum,
                    height=max(400, len(cum_matrix) * 22),
                    xaxis={'title': 'Months Since First Purchase', 'side': 'bottom'},
                    yaxis={'title': '', 'autorange': 'reversed'},
                    margin=dict(l=80, r=20, t=20, b=60),
                )
                st.plotly_chart(fig_cum, use_container_width=True)

            # Revenue per customer by cohort
            st.markdown('<p class="section-header">Revenue per Customer by Cohort Month</p>', unsafe_allow_html=True)

            rev_pivot = cohort_df.pivot_table(
                index='cohort',
                columns='months_since_first',
                values='revenue_per_customer',
                aggfunc='sum'
            ).fillna(0).astype(float)

            # Cumulative revenue per customer
            rev_cumulative = rev_pivot.cumsum(axis=1)
            recent_cohorts = rev_cumulative.tail(12)

            if not recent_cohorts.empty and len(recent_cohorts) > 0:
                fig2 = go.Figure()
                n_cohorts = max(len(recent_cohorts), 2)
                colors = px.colors.sample_colorscale('Plasma', np.linspace(0, 0.9, n_cohorts))
                for i, (cohort, row) in enumerate(recent_cohorts.iterrows()):
                    vals = row[row > 0]
                    if len(vals) == 0:
                        continue
                    fig2.add_trace(go.Scatter(
                        x=[f'M{c}' for c in vals.index],
                        y=vals.values.astype(float),
                        name=str(cohort),
                        mode='lines',
                        line=dict(color=colors[i], width=2),
                        hovertemplate=str(cohort) + '<br>Month %{x}<br>$%{y:,.0f}<extra></extra>'
                    ))

                apply_dark_theme(fig2, height=400,
                    legend=dict(orientation="v", yanchor="top", y=1, xanchor="left", x=1.02, font=dict(color='#6B6560', size=10)),
                    xaxis={'title': 'Months Since First Purchase'},
                    yaxis={'title': 'Cumulative Revenue / Customer ($)'}
                )
                st.plotly_chart(fig2, use_container_width=True)

    # === Tab 4: Top Customers ===
    with tab4:
        st.markdown('<p class="section-header">Top 50 Customers by Lifetime Revenue</p>', unsafe_allow_html=True)

        try:
            top_df = load_top_customers(platform, ds, de)
        except Exception as e:
            st.error(f"Error: {e}")
            top_df = pd.DataFrame()

        if not top_df.empty:
            display_df = top_df.copy()
            display_df['lifetime_revenue'] = display_df['lifetime_revenue'].apply(lambda x: f"${_f(x):,.2f}")
            display_df['platform'] = display_df.apply(
                lambda r: 'Both' if _f(r['woo_orders']) > 0 and _f(r['shopify_orders']) > 0
                else ('Woo' if _f(r['woo_orders']) > 0 else 'Shopify'),
                axis=1
            )
            display_df = display_df[['email', 'lifetime_orders', 'lifetime_revenue', 'first_order', 'last_order', 'platform', 'refunded_orders']]
            display_df.columns = ['Email', 'Orders', 'Revenue', 'First Order', 'Last Order', 'Platform', 'Refunds']

            st.dataframe(display_df, use_container_width=True, hide_index=True, height=600)

    # Footer
    st.markdown("""
    <div style="text-align: center; color: #6B6560; margin-top: 48px; padding: 24px; border-top: 1px solid rgba(45,41,38,0.1);">
        <p style="margin: 0;">Data: WooCommerce (Mar 2023 - Sep 2025) + Shopify (Jun 2025+)</p>
        <p style="margin: 4px 0 0 0; font-size: 12px;">Overlap period (Jun-Sep 2025) deduplicated by email + date + amount</p>
    </div>
    """, unsafe_allow_html=True)


main()
