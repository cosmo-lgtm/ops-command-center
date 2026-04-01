"""
D2C Customer LTV Dashboard
WooCommerce (2023-2025) + Shopify (2025+) unified customer lifetime value analytics.
Cohort analysis, retention, platform migration tracking.
"""

import streamlit as st
import pandas as pd
from google.cloud import bigquery
from datetime import datetime
import plotly.graph_objects as go
import plotly.express as px
import numpy as np

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
        background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin: 0;
    }

    .metric-value-teal {
        font-size: 36px;
        font-weight: 700;
        background: linear-gradient(135deg, #00d4aa 0%, #00a3cc 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin: 0;
    }

    .metric-value-gold {
        font-size: 36px;
        font-weight: 700;
        background: linear-gradient(135deg, #ffd666 0%, #ff9f43 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin: 0;
    }

    .metric-value-blue {
        font-size: 36px;
        font-weight: 700;
        background: linear-gradient(135deg, #74b9ff 0%, #667eea 100%);
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

    .metric-sub {
        font-size: 12px;
        color: #5a6a8a;
        margin-top: 4px;
    }

    .dashboard-header {
        background: linear-gradient(90deg, #f093fb 0%, #f5576c 100%);
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
        border-bottom: 2px solid rgba(240, 147, 251, 0.3);
    }

    .insight-banner {
        background: linear-gradient(145deg, #1e3a5f 0%, #2a4a6a 100%);
        border-radius: 12px;
        padding: 16px 24px;
        border-left: 4px solid #f093fb;
        margin: 16px 0;
        color: #ccd6f6;
    }
</style>
""", unsafe_allow_html=True)

COLORS = {
    'primary': '#f093fb',
    'secondary': '#f5576c',
    'teal': '#00d4aa',
    'gold': '#ffd666',
    'blue': '#74b9ff',
    'purple': '#667eea',
    'woo': '#96588a',
    'shopify': '#95bf47',
}


def apply_dark_theme(fig, height=350, **kwargs):
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


# --- Data Loaders ---

@st.cache_data(ttl=600)
def load_ltv_summary():
    client = get_bq_client()
    query = """
    SELECT
        COUNT(*) AS total_customers,
        ROUND(SUM(lifetime_revenue), 2) AS total_revenue,
        ROUND(AVG(lifetime_revenue), 2) AS avg_ltv,
        ROUND(APPROX_QUANTILES(lifetime_revenue, 100)[OFFSET(50)], 2) AS median_ltv,
        ROUND(AVG(lifetime_orders), 1) AS avg_orders,
        COUNTIF(lifetime_orders > 1) AS repeat_customers,
        COUNTIF(woo_orders > 0 AND shopify_orders > 0) AS cross_platform,
        ROUND(SUM(woo_revenue), 2) AS woo_revenue,
        ROUND(SUM(shopify_revenue), 2) AS shopify_revenue,
        COUNTIF(woo_orders > 0) AS woo_customers,
        COUNTIF(shopify_orders > 0) AS shopify_customers,
        ROUND(AVG(customer_lifespan_days), 0) AS avg_lifespan_days,
        COUNTIF(refunded_orders > 0) AS customers_with_refunds
    FROM `artful-logic-475116-p1.marts.vw_d2c_customer_ltv`
    """
    return client.query(query).to_dataframe()


@st.cache_data(ttl=600)
def load_ltv_distribution():
    client = get_bq_client()
    query = """
    SELECT
        CASE
            WHEN lifetime_revenue < 25 THEN '$0-25'
            WHEN lifetime_revenue < 50 THEN '$25-50'
            WHEN lifetime_revenue < 100 THEN '$50-100'
            WHEN lifetime_revenue < 200 THEN '$100-200'
            WHEN lifetime_revenue < 500 THEN '$200-500'
            WHEN lifetime_revenue < 1000 THEN '$500-1K'
            ELSE '$1K+'
        END AS ltv_bucket,
        CASE
            WHEN lifetime_revenue < 25 THEN 1
            WHEN lifetime_revenue < 50 THEN 2
            WHEN lifetime_revenue < 100 THEN 3
            WHEN lifetime_revenue < 200 THEN 4
            WHEN lifetime_revenue < 500 THEN 5
            WHEN lifetime_revenue < 1000 THEN 6
            ELSE 7
        END AS bucket_order,
        COUNT(*) AS customers,
        ROUND(SUM(lifetime_revenue), 0) AS bucket_revenue
    FROM `artful-logic-475116-p1.marts.vw_d2c_customer_ltv`
    GROUP BY 1, 2
    ORDER BY 2
    """
    return client.query(query).to_dataframe()


@st.cache_data(ttl=600)
def load_monthly_revenue():
    client = get_bq_client()
    query = """
    WITH woo AS (
        SELECT
            DATE_TRUNC(TIMESTAMP(date_created), MONTH) AS month,
            COUNT(*) AS orders,
            SUM(CAST(total AS NUMERIC)) AS revenue,
            'WooCommerce' AS platform
        FROM `artful-logic-475116-p1.raw_woocommerce.orders`
        WHERE status IN ('completed', 'refunded')
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
            AND cancelled_at IS NULL
        GROUP BY 1
    )
    SELECT * FROM woo
    UNION ALL
    SELECT * FROM shopify
    ORDER BY month, platform
    """
    return client.query(query).to_dataframe()


@st.cache_data(ttl=600)
def load_cohort_data():
    client = get_bq_client()
    query = """
    SELECT
        FORMAT_TIMESTAMP('%Y-%m', cohort_month) AS cohort,
        months_since_first,
        customers,
        orders,
        revenue,
        revenue_per_customer
    FROM `artful-logic-475116-p1.marts.vw_d2c_cohort_analysis`
    WHERE months_since_first <= 18
    ORDER BY cohort, months_since_first
    """
    return client.query(query).to_dataframe()


@st.cache_data(ttl=600)
def load_repeat_rate_trend():
    client = get_bq_client()
    query = """
    WITH customer_cohorts AS (
        SELECT
            email,
            DATE_TRUNC(first_order_date, QUARTER) AS acquisition_quarter,
            lifetime_orders
        FROM `artful-logic-475116-p1.marts.vw_d2c_customer_ltv`
    )
    SELECT
        FORMAT_TIMESTAMP('%Y-Q%Q', acquisition_quarter) AS quarter,
        COUNT(*) AS total_customers,
        COUNTIF(lifetime_orders > 1) AS repeat_customers,
        ROUND(COUNTIF(lifetime_orders > 1) / COUNT(*) * 100, 1) AS repeat_rate,
        ROUND(AVG(lifetime_orders), 2) AS avg_orders
    FROM customer_cohorts
    GROUP BY 1, acquisition_quarter
    ORDER BY acquisition_quarter
    """
    return client.query(query).to_dataframe()


@st.cache_data(ttl=600)
def load_top_customers():
    client = get_bq_client()
    query = """
    SELECT
        email,
        lifetime_orders,
        ROUND(lifetime_revenue, 2) AS lifetime_revenue,
        DATE(first_order) AS first_order,
        DATE(last_order) AS last_order,
        woo_orders,
        shopify_orders,
        refunded_orders
    FROM `artful-logic-475116-p1.marts.vw_d2c_customer_ltv`
    ORDER BY lifetime_revenue DESC
    LIMIT 50
    """
    return client.query(query).to_dataframe()


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
    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 32px;">
        <div>
            <h1 class="dashboard-header">D2C Customer LTV</h1>
            <p class="dashboard-subtitle">WooCommerce (2023-2025) + Shopify (2025+) &mdash; Unified Customer Lifetime Value</p>
        </div>
    </div>
    """, unsafe_allow_html=True)

    try:
        summary = load_ltv_summary()
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return

    if summary.empty:
        st.warning("No LTV data available. Ensure the BQ views exist.")
        return

    s = summary.iloc[0].apply(lambda v: float(v) if hasattr(v, '__float__') else v)
    repeat_rate = (s['repeat_customers'] / s['total_customers'] * 100) if s['total_customers'] else 0

    # --- KPI Row ---
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    with c1:
        st.markdown(render_metric(f"${s['total_revenue']/1e6:.1f}M", "Lifetime Revenue"), unsafe_allow_html=True)
    with c2:
        st.markdown(render_metric(f"{s['total_customers']:,.0f}", "Unique Customers", style="teal"), unsafe_allow_html=True)
    with c3:
        st.markdown(render_metric(f"${s['avg_ltv']:.0f}", "Avg LTV", style="gold", sub=f"Median: ${s['median_ltv']:.0f}"), unsafe_allow_html=True)
    with c4:
        st.markdown(render_metric(f"{s['avg_orders']:.1f}", "Avg Orders", style="blue"), unsafe_allow_html=True)
    with c5:
        st.markdown(render_metric(f"{repeat_rate:.1f}%", "Repeat Rate", style="teal", sub=f"{s['repeat_customers']:,.0f} repeat"), unsafe_allow_html=True)
    with c6:
        st.markdown(render_metric(f"{s['cross_platform']:,.0f}", "Cross-Platform", style="gold", sub="Woo + Shopify"), unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # --- Platform Insight Banner ---
    woo_pct = s['woo_revenue'] / s['total_revenue'] * 100 if s['total_revenue'] else 0
    shopify_pct = 100 - woo_pct
    st.markdown(f"""
    <div class="insight-banner">
        <strong style="color: #f093fb;">Platform Split:</strong>
        <span style="color: {COLORS['woo']}; margin-left: 12px;">WooCommerce</span>
        ${s['woo_revenue']/1e6:.1f}M ({woo_pct:.0f}%) &bull; {s['woo_customers']:,.0f} customers
        &nbsp;&nbsp;|&nbsp;&nbsp;
        <span style="color: {COLORS['shopify']};">Shopify</span>
        ${s['shopify_revenue']/1e6:.1f}M ({shopify_pct:.0f}%) &bull; {s['shopify_customers']:,.0f} customers
    </div>
    """, unsafe_allow_html=True)

    # --- Tabs ---
    tab1, tab2, tab3, tab4 = st.tabs(["Revenue Timeline", "LTV Distribution", "Cohort Retention", "Top Customers"])

    # === Tab 1: Revenue Timeline ===
    with tab1:
        st.markdown('<p class="section-header">Monthly Revenue by Platform</p>', unsafe_allow_html=True)

        try:
            monthly = load_monthly_revenue()
        except Exception as e:
            st.error(f"Error: {e}")
            monthly = pd.DataFrame()

        if not monthly.empty:
            monthly['month'] = pd.to_datetime(monthly['month'])

            fig = go.Figure()
            for platform, color in [('WooCommerce', COLORS['woo']), ('Shopify', COLORS['shopify'])]:
                df = monthly[monthly['platform'] == platform]
                fig.add_trace(go.Bar(
                    x=df['month'],
                    y=df['revenue'],
                    name=platform,
                    marker_color=color,
                    hovertemplate='%{x|%b %Y}<br>$%{y:,.0f}<extra>' + platform + '</extra>'
                ))

            apply_dark_theme(fig, height=400,
                barmode='stack',
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font=dict(color='#8892b0')),
                xaxis={'title': ''},
                yaxis={'title': 'Revenue ($)'}
            )
            st.plotly_chart(fig, use_container_width=True)

            # Orders trend (secondary)
            st.markdown('<p class="section-header">Monthly Order Volume</p>', unsafe_allow_html=True)
            fig2 = go.Figure()
            for platform, color in [('WooCommerce', COLORS['woo']), ('Shopify', COLORS['shopify'])]:
                df = monthly[monthly['platform'] == platform]
                fig2.add_trace(go.Scatter(
                    x=df['month'],
                    y=df['orders'],
                    name=platform,
                    mode='lines+markers',
                    line=dict(color=color, width=2),
                    marker=dict(size=5),
                    hovertemplate='%{x|%b %Y}<br>%{y:,.0f} orders<extra>' + platform + '</extra>'
                ))

            apply_dark_theme(fig2, height=300,
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font=dict(color='#8892b0')),
                yaxis={'title': 'Orders'}
            )
            st.plotly_chart(fig2, use_container_width=True)

    # === Tab 2: LTV Distribution ===
    with tab2:
        st.markdown('<p class="section-header">Customer LTV Distribution</p>', unsafe_allow_html=True)

        try:
            dist = load_ltv_distribution()
        except Exception as e:
            st.error(f"Error: {e}")
            dist = pd.DataFrame()

        if not dist.empty:
            col1, col2 = st.columns(2)

            with col1:
                fig = go.Figure(go.Bar(
                    x=dist['ltv_bucket'],
                    y=dist['customers'],
                    marker=dict(
                        color=dist['bucket_order'],
                        colorscale=[[0, COLORS['blue']], [0.5, COLORS['primary']], [1, COLORS['secondary']]],
                    ),
                    text=dist['customers'].apply(lambda x: f'{x:,.0f}'),
                    textposition='outside',
                    textfont=dict(color='#ccd6f6', size=11),
                    hovertemplate='%{x}<br>%{y:,.0f} customers<extra></extra>'
                ))
                apply_dark_theme(fig, height=350, yaxis={'title': 'Customers'})
                fig.update_layout(title=dict(text='Customers by LTV Bucket', font=dict(color='#8892b0', size=14)))
                st.plotly_chart(fig, use_container_width=True)

            with col2:
                fig2 = go.Figure(go.Bar(
                    x=dist['ltv_bucket'],
                    y=dist['bucket_revenue'],
                    marker=dict(
                        color=dist['bucket_order'],
                        colorscale=[[0, '#ffd666'], [0.5, '#ff9f43'], [1, '#f5576c']],
                    ),
                    text=dist['bucket_revenue'].apply(lambda x: f'${x/1000:.0f}K'),
                    textposition='outside',
                    textfont=dict(color='#ccd6f6', size=11),
                    hovertemplate='%{x}<br>$%{y:,.0f} revenue<extra></extra>'
                ))
                apply_dark_theme(fig2, height=350, yaxis={'title': 'Revenue ($)'})
                fig2.update_layout(title=dict(text='Revenue by LTV Bucket', font=dict(color='#8892b0', size=14)))
                st.plotly_chart(fig2, use_container_width=True)

            # Key insight
            top_bucket = dist[dist['ltv_bucket'] == '$1K+']
            if not top_bucket.empty:
                top_cust = top_bucket['customers'].iloc[0]
                top_rev = top_bucket['bucket_revenue'].iloc[0]
                top_pct = top_cust / dist['customers'].sum() * 100
                rev_pct = top_rev / dist['bucket_revenue'].sum() * 100
                st.markdown(f"""
                <div class="insight-banner">
                    <strong style="color: #ffd666;">$1K+ Customers:</strong>
                    {top_cust:,.0f} customers ({top_pct:.1f}%) drive ${top_rev/1e6:.1f}M ({rev_pct:.0f}%) of total revenue
                </div>
                """, unsafe_allow_html=True)

        # Repeat rate by acquisition cohort
        st.markdown('<p class="section-header">Repeat Rate by Acquisition Quarter</p>', unsafe_allow_html=True)
        try:
            repeat_df = load_repeat_rate_trend()
        except Exception as e:
            st.error(f"Error: {e}")
            repeat_df = pd.DataFrame()

        if not repeat_df.empty:
            fig3 = go.Figure()
            fig3.add_trace(go.Bar(
                x=repeat_df['quarter'],
                y=repeat_df['total_customers'],
                name='Total Customers',
                marker_color='rgba(116, 185, 255, 0.3)',
                yaxis='y2',
                hovertemplate='%{x}<br>%{y:,.0f} customers<extra></extra>'
            ))
            fig3.add_trace(go.Scatter(
                x=repeat_df['quarter'],
                y=repeat_df['repeat_rate'],
                name='Repeat Rate %',
                mode='lines+markers+text',
                line=dict(color=COLORS['primary'], width=3),
                marker=dict(size=8),
                text=repeat_df['repeat_rate'].apply(lambda x: f'{x:.0f}%'),
                textposition='top center',
                textfont=dict(color='#ccd6f6', size=10),
                hovertemplate='%{x}<br>%{y:.1f}% repeat<extra></extra>'
            ))

            apply_dark_theme(fig3, height=350,
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font=dict(color='#8892b0')),
                yaxis={'title': 'Repeat Rate (%)', 'range': [0, 100]},
                yaxis2=dict(overlaying='y', side='right', showgrid=False, title='Customers', titlefont=dict(color='#8892b0'), tickfont=dict(color='#5a6a8a'))
            )
            st.plotly_chart(fig3, use_container_width=True)

    # === Tab 3: Cohort Retention ===
    with tab3:
        st.markdown('<p class="section-header">Cohort Retention Heatmap</p>', unsafe_allow_html=True)

        try:
            cohort_df = load_cohort_data()
        except Exception as e:
            st.error(f"Error: {e}")
            cohort_df = pd.DataFrame()

        if not cohort_df.empty:
            # Build retention matrix
            pivot = cohort_df.pivot_table(
                index='cohort',
                columns='months_since_first',
                values='customers',
                aggfunc='sum'
            ).fillna(0)

            # Calculate retention percentages
            month_0 = pivot[0]
            retention_pct = pivot.div(month_0, axis=0) * 100

            # Only show cohorts with at least some data
            retention_pct = retention_pct[month_0 > 50]

            if not retention_pct.empty:
                fig = go.Figure(data=go.Heatmap(
                    z=retention_pct.values,
                    x=[f'M{i}' for i in retention_pct.columns],
                    y=retention_pct.index,
                    colorscale=[
                        [0, '#0f0f1a'],
                        [0.05, '#1a1a3e'],
                        [0.15, '#2a2a6a'],
                        [0.3, '#667eea'],
                        [0.5, '#f093fb'],
                        [1.0, '#f5576c']
                    ],
                    text=retention_pct.values.round(1).astype(str) + '%',
                    texttemplate='%{text}',
                    textfont=dict(size=9, color='#ccd6f6'),
                    hovertemplate='Cohort: %{y}<br>Month: %{x}<br>Retention: %{z:.1f}%<extra></extra>',
                    colorbar=dict(
                        title='Retention %',
                        titlefont=dict(color='#8892b0'),
                        tickfont=dict(color='#8892b0')
                    )
                ))

                apply_dark_theme(fig, height=max(400, len(retention_pct) * 22),
                    xaxis={'title': 'Months Since First Purchase', 'side': 'bottom'},
                    yaxis={'title': '', 'autorange': 'reversed'},
                    margin=dict(l=80, r=20, t=20, b=60)
                )
                st.plotly_chart(fig, use_container_width=True)

            # Revenue per customer by cohort
            st.markdown('<p class="section-header">Revenue per Customer by Cohort Month</p>', unsafe_allow_html=True)

            rev_pivot = cohort_df.pivot_table(
                index='cohort',
                columns='months_since_first',
                values='revenue_per_customer',
                aggfunc='sum'
            ).fillna(0)

            # Cumulative revenue per customer
            rev_cumulative = rev_pivot.cumsum(axis=1)
            recent_cohorts = rev_cumulative.tail(12)

            if not recent_cohorts.empty:
                fig2 = go.Figure()
                colors = px.colors.sample_colorscale('Plasma', np.linspace(0, 0.9, len(recent_cohorts)))
                for i, (cohort, row) in enumerate(recent_cohorts.iterrows()):
                    vals = row[row > 0]
                    fig2.add_trace(go.Scatter(
                        x=[f'M{c}' for c in vals.index],
                        y=vals.values,
                        name=cohort,
                        mode='lines',
                        line=dict(color=colors[i], width=2),
                        hovertemplate=f'{cohort}<br>Month %{{x}}<br>${{%{{y:,.0f}}}}<extra></extra>'
                    ))

                apply_dark_theme(fig2, height=400,
                    legend=dict(orientation="v", yanchor="top", y=1, xanchor="left", x=1.02, font=dict(color='#8892b0', size=10)),
                    xaxis={'title': 'Months Since First Purchase'},
                    yaxis={'title': 'Cumulative Revenue / Customer ($)'}
                )
                st.plotly_chart(fig2, use_container_width=True)

    # === Tab 4: Top Customers ===
    with tab4:
        st.markdown('<p class="section-header">Top 50 Customers by Lifetime Revenue</p>', unsafe_allow_html=True)

        try:
            top_df = load_top_customers()
        except Exception as e:
            st.error(f"Error: {e}")
            top_df = pd.DataFrame()

        if not top_df.empty:
            display_df = top_df.copy()
            display_df['lifetime_revenue'] = display_df['lifetime_revenue'].apply(lambda x: f"${x:,.2f}")
            display_df['platform'] = display_df.apply(
                lambda r: 'Both' if r['woo_orders'] > 0 and r['shopify_orders'] > 0
                else ('Woo' if r['woo_orders'] > 0 else 'Shopify'),
                axis=1
            )
            display_df = display_df[['email', 'lifetime_orders', 'lifetime_revenue', 'first_order', 'last_order', 'platform', 'refunded_orders']]
            display_df.columns = ['Email', 'Orders', 'Revenue', 'First Order', 'Last Order', 'Platform', 'Refunds']

            st.dataframe(display_df, use_container_width=True, hide_index=True, height=600)

    # Footer
    st.markdown(f"""
    <div style="text-align: center; color: #8892b0; margin-top: 48px; padding: 24px; border-top: 1px solid rgba(255,255,255,0.1);">
        <p style="margin: 0;">Data: WooCommerce (Mar 2023 - Sep 2025) + Shopify (Jun 2025+)</p>
        <p style="margin: 4px 0 0 0; font-size: 12px;">Overlap period (Jun-Sep 2025) deduplicated by email + date + amount</p>
    </div>
    """, unsafe_allow_html=True)


main()
