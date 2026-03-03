"""
Market Intelligence Dashboard
CT Cannabis Market Data • Industry Trends • Competitive Analysis
"""

import streamlit as st
import pandas as pd
from google.cloud import bigquery
from datetime import datetime, timedelta
import plotly.graph_objects as go
import plotly.express as px

# Page config - MUST be first Streamlit command
st.set_page_config(
    page_title="Market Intelligence",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Dark mode custom CSS with gradient accents
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

    /* Metric cards */
    .metric-card {
        background: linear-gradient(145deg, #1e1e2f 0%, #2a2a4a 100%);
        border-radius: 16px;
        padding: 24px;
        border: 1px solid rgba(255,255,255,0.1);
        box-shadow: 0 8px 32px rgba(0,0,0,0.3);
        margin-bottom: 16px;
        transition: transform 0.3s ease, box-shadow 0.3s ease;
    }
    .metric-card:hover {
        transform: translateY(-4px);
        box-shadow: 0 16px 48px rgba(0,0,0,0.5);
    }

    /* Premium metric styling */
    .metric-value {
        font-size: clamp(2rem, 5vw, 3rem);
        font-weight: 800;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin: 0;
        line-height: 1.1;
    }
    .metric-value-green {
        font-size: clamp(2rem, 5vw, 3rem);
        font-weight: 800;
        background: linear-gradient(135deg, #00f5a0 0%, #00d9f5 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin: 0;
        line-height: 1.1;
    }
    .metric-value-gold {
        font-size: clamp(2rem, 5vw, 3rem);
        font-weight: 800;
        background: linear-gradient(135deg, #f5af19 0%, #f12711 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin: 0;
        line-height: 1.1;
    }
    .metric-value-cyan {
        font-size: clamp(2rem, 5vw, 3rem);
        font-weight: 800;
        background: linear-gradient(135deg, #00d2ff 0%, #3a7bd5 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin: 0;
        line-height: 1.1;
    }
    .metric-label {
        font-size: clamp(0.75rem, 1.5vw, 0.875rem);
        color: #8892b0;
        text-transform: uppercase;
        letter-spacing: 1.5px;
        margin-top: 8px;
        font-weight: 600;
    }
    .metric-sublabel {
        font-size: clamp(0.7rem, 1.2vw, 0.8rem);
        color: #5a6785;
        margin-top: 4px;
    }

    /* Headers */
    .dashboard-header {
        background: linear-gradient(90deg, #00f5a0 0%, #00d9f5 50%, #667eea 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-size: clamp(2rem, 5vw, 3rem);
        font-weight: 800;
        margin-bottom: 8px;
    }
    .dashboard-subtitle {
        color: #8892b0;
        font-size: clamp(0.875rem, 2vw, 1.125rem);
        margin-bottom: 32px;
    }
    .section-header {
        color: #ccd6f6;
        font-size: clamp(1.25rem, 2.5vw, 1.5rem);
        font-weight: 700;
        margin: 32px 0 16px 0;
        padding-bottom: 12px;
        border-bottom: 3px solid;
        border-image: linear-gradient(90deg, #667eea, #764ba2) 1;
    }

    /* Live indicator */
    .live-indicator {
        display: inline-flex;
        align-items: center;
        gap: 8px;
        color: #00f5a0;
        font-size: clamp(0.65rem, 1.5vw, 0.75rem);
        text-transform: uppercase;
        letter-spacing: 2px;
        font-weight: 600;
    }
    .live-dot {
        width: 10px;
        height: 10px;
        background: linear-gradient(135deg, #00f5a0, #00d9f5);
        border-radius: 50%;
        animation: pulse 2s infinite;
        box-shadow: 0 0 20px rgba(0, 245, 160, 0.5);
    }
    @keyframes pulse {
        0%, 100% { opacity: 1; transform: scale(1); box-shadow: 0 0 20px rgba(0, 245, 160, 0.5); }
        50% { opacity: 0.7; transform: scale(1.3); box-shadow: 0 0 30px rgba(0, 245, 160, 0.8); }
    }

    /* Market share bars */
    .market-bar-container {
        background: linear-gradient(145deg, #1e1e2f 0%, #2a2a4a 100%);
        border-radius: 12px;
        padding: 16px 20px;
        margin-bottom: 12px;
        border: 1px solid rgba(255,255,255,0.05);
    }
    .market-bar-label {
        color: #ccd6f6;
        font-weight: 600;
        font-size: 0.9rem;
        margin-bottom: 8px;
        display: flex;
        justify-content: space-between;
    }
    .market-bar {
        height: 12px;
        background: rgba(255,255,255,0.1);
        border-radius: 6px;
        overflow: hidden;
    }
    .market-bar-fill {
        height: 100%;
        border-radius: 6px;
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        transition: width 1s ease;
    }

    /* Data source badge */
    .data-badge {
        background: rgba(102, 126, 234, 0.2);
        color: #667eea;
        padding: 6px 16px;
        border-radius: 20px;
        font-size: 0.75rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 1px;
        display: inline-block;
        margin-bottom: 16px;
    }

    /* Trend indicators */
    .trend-up { color: #00f5a0; }
    .trend-down { color: #ff6b6b; }
    .trend-flat { color: #ffd666; }

    /* Responsive */
    @media (max-width: 992px) {
        .block-container { padding-left: 1rem !important; padding-right: 1rem !important; }
    }
    @media (max-width: 640px) {
        .block-container { padding-left: 0.5rem !important; padding-right: 0.5rem !important; }
        .metric-card { padding: 16px; border-radius: 12px; }
    }
</style>
""", unsafe_allow_html=True)

COLORS = {
    'primary': '#667eea',
    'secondary': '#764ba2',
    'success': '#00f5a0',
    'cyan': '#00d9f5',
    'warning': '#ffd666',
    'danger': '#ff6b6b',
    'gold': '#f5af19',
    'purple': '#a855f7'
}

GRADIENT_COLORS = [
    '#667eea', '#764ba2', '#a855f7', '#00d9f5', '#00f5a0'
]


def apply_dark_theme(fig, height=400, **kwargs):
    layout_args = {
        'paper_bgcolor': 'rgba(0,0,0,0)',
        'plot_bgcolor': 'rgba(0,0,0,0)',
        'font': {'color': '#ccd6f6', 'family': 'Inter, sans-serif', 'size': 12},
        'height': height,
        'margin': kwargs.get('margin', dict(l=40, r=40, t=40, b=40)),
        'xaxis': {
            'gridcolor': 'rgba(255,255,255,0.08)',
            'linecolor': 'rgba(255,255,255,0.1)',
            'tickfont': {'color': '#8892b0', 'size': 11},
            'title_font': {'color': '#ccd6f6', 'size': 12},
            **kwargs.get('xaxis', {})
        },
        'yaxis': {
            'gridcolor': 'rgba(255,255,255,0.08)',
            'linecolor': 'rgba(255,255,255,0.1)',
            'tickfont': {'color': '#8892b0', 'size': 11},
            'title_font': {'color': '#ccd6f6', 'size': 12},
            **kwargs.get('yaxis', {})
        },
        'legend': {
            'bgcolor': 'rgba(0,0,0,0)',
            'font': {'color': '#ccd6f6', 'size': 11}
        },
        'hovermode': 'x unified',
        'hoverlabel': {
            'bgcolor': '#1e1e2f',
            'bordercolor': '#667eea',
            'font': {'color': '#ccd6f6', 'size': 12}
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
        if "gcp_service_account" in st.secrets:
            from google.oauth2 import service_account
            credentials = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])
            return bigquery.Client(project='artful-logic-475116-p1', credentials=credentials)
    except Exception:
        pass  # Fall through to use application default credentials
    return bigquery.Client(project='artful-logic-475116-p1')


@st.cache_data(ttl=3600)  # Cache for 1 hour
def load_weekly_sales():
    """Load CT weekly cannabis sales data"""
    client = get_bq_client()
    query = """
    SELECT
        unnamed_column as week_date,
        adult_use as adult_use_sales,
        medical as medical_sales,
        total as total_sales,
        adult_use_products_sold,
        medical_products_sold,
        total_products_sold,
        adult_use_cannabis_average_product_price as avg_adult_price,
        medical_marijuana_average_product_price as avg_medical_price
    FROM `artful-logic-475116-p1.market_data.ct_weekly_sales`
    WHERE unnamed_column IS NOT NULL
    ORDER BY unnamed_column
    """
    return client.query(query).to_dataframe()


@st.cache_data(ttl=3600)
def load_thc_beverage_sales():
    """Load CT THC beverage distributor sales data"""
    client = get_bq_client()
    query = """
    SELECT
        business as distributor_name,
        containers_sold,
        dates_covered,
        credential
    FROM `artful-logic-475116-p1.market_data.ct_thc_beverage_sales`
    WHERE containers_sold > 0
    ORDER BY containers_sold DESC
    """
    return client.query(query).to_dataframe()


def format_currency(value, decimals=1):
    """Format currency with M/K suffix"""
    if pd.isna(value) or value is None:
        return "N/A"
    if value >= 1_000_000:
        return f"${value/1_000_000:.{decimals}f}M"
    elif value >= 1_000:
        return f"${value/1_000:.{decimals}f}K"
    else:
        return f"${value:.0f}"


def format_number(value, decimals=0):
    """Format large numbers with K/M suffix"""
    if pd.isna(value) or value is None:
        return "N/A"
    if value >= 1_000_000:
        return f"{value/1_000_000:.{decimals}f}M"
    elif value >= 1_000:
        return f"{value/1_000:.{decimals}f}K"
    else:
        return f"{value:.0f}"


def main():
    # Header
    st.markdown("""
    <div style="margin-bottom: 32px;">
        <h1 class="dashboard-header">Market Intelligence</h1>
        <p class="dashboard-subtitle">Connecticut Cannabis Market Analytics • THC Beverage Competitive Landscape</p>
        <div class="live-indicator">
            <span class="live-dot"></span>
            Public Data • CT Open Data Portal
        </div>
    </div>
    """, unsafe_allow_html=True)

    # Load data
    try:
        weekly_df = load_weekly_sales()
        beverage_df = load_thc_beverage_sales()
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return

    if weekly_df.empty:
        st.warning("No weekly sales data available")
        return

    # Calculate key metrics
    latest_week = weekly_df.iloc[-1]
    prev_week = weekly_df.iloc[-2] if len(weekly_df) > 1 else latest_week

    # Trailing 4 weeks avg
    last_4_weeks = weekly_df.tail(4)
    avg_weekly_sales = last_4_weeks['total_sales'].mean()
    avg_products = last_4_weeks['total_products_sold'].mean()

    # YTD totals
    current_year = datetime.now().year
    weekly_df['year'] = pd.to_datetime(weekly_df['week_date']).dt.year
    ytd_df = weekly_df[weekly_df['year'] == current_year]
    ytd_sales = ytd_df['total_sales'].sum()
    ytd_products = ytd_df['total_products_sold'].sum()

    # Week-over-week change
    wow_change = ((latest_week['total_sales'] - prev_week['total_sales']) / prev_week['total_sales'] * 100) if prev_week['total_sales'] > 0 else 0

    # ==========================================
    # KEY METRICS ROW
    # ==========================================
    st.markdown('<div class="section-header">Market Overview</div>', unsafe_allow_html=True)

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value-green">{format_currency(avg_weekly_sales)}</div>
            <div class="metric-label">Avg Weekly Sales</div>
            <div class="metric-sublabel">Last 4 weeks</div>
        </div>
        """, unsafe_allow_html=True)

    with col2:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value-cyan">{format_number(avg_products)}</div>
            <div class="metric-label">Products/Week</div>
            <div class="metric-sublabel">Market volume</div>
        </div>
        """, unsafe_allow_html=True)

    with col3:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value-gold">{format_currency(ytd_sales)}</div>
            <div class="metric-label">YTD Total Sales</div>
            <div class="metric-sublabel">{current_year} cumulative</div>
        </div>
        """, unsafe_allow_html=True)

    with col4:
        trend_class = "trend-up" if wow_change > 0 else "trend-down" if wow_change < 0 else "trend-flat"
        trend_icon = "↑" if wow_change > 0 else "↓" if wow_change < 0 else "→"
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">${latest_week['avg_adult_price']:.0f}</div>
            <div class="metric-label">Avg Product Price</div>
            <div class="metric-sublabel"><span class="{trend_class}">{trend_icon} {abs(wow_change):.1f}% WoW</span></div>
        </div>
        """, unsafe_allow_html=True)

    # ==========================================
    # WEEKLY SALES TREND CHART
    # ==========================================
    st.markdown('<div class="section-header">Weekly Cannabis Sales Trend</div>', unsafe_allow_html=True)

    # Create the main chart
    fig = go.Figure()

    # Add area fill for total sales
    fig.add_trace(go.Scatter(
        x=weekly_df['week_date'],
        y=weekly_df['total_sales'] / 1_000_000,
        fill='tozeroy',
        fillcolor='rgba(102, 126, 234, 0.2)',
        line=dict(color='#667eea', width=0),
        name='Total Sales',
        hovertemplate='%{y:.2f}M<extra></extra>'
    ))

    # Add adult use line
    fig.add_trace(go.Scatter(
        x=weekly_df['week_date'],
        y=weekly_df['adult_use_sales'] / 1_000_000,
        mode='lines',
        line=dict(color='#00f5a0', width=3),
        name='Adult Use',
        hovertemplate='%{y:.2f}M<extra></extra>'
    ))

    # Add medical line
    fig.add_trace(go.Scatter(
        x=weekly_df['week_date'],
        y=weekly_df['medical_sales'] / 1_000_000,
        mode='lines',
        line=dict(color='#764ba2', width=3),
        name='Medical',
        hovertemplate='%{y:.2f}M<extra></extra>'
    ))

    fig = apply_dark_theme(fig, height=450,
        yaxis={'title': 'Sales ($M)', 'tickprefix': '$', 'ticksuffix': 'M'},
        xaxis={'title': ''},
        legend={'orientation': 'h', 'yanchor': 'bottom', 'y': 1.02, 'xanchor': 'right', 'x': 1}
    )

    st.plotly_chart(fig, use_container_width=True)

    # ==========================================
    # SPLIT: Adult vs Medical | THC Beverage Market
    # ==========================================
    col_left, col_right = st.columns([1.2, 1])

    with col_left:
        st.markdown('<div class="section-header">Adult Use vs Medical Split</div>', unsafe_allow_html=True)

        # Pie chart with donut style
        adult_total = ytd_df['adult_use_sales'].sum()
        medical_total = ytd_df['medical_sales'].sum()

        fig_pie = go.Figure(data=[go.Pie(
            labels=['Adult Use', 'Medical'],
            values=[adult_total, medical_total],
            hole=0.65,
            marker=dict(colors=['#00f5a0', '#764ba2']),
            textinfo='percent',
            textfont=dict(size=14, color='#ccd6f6'),
            hovertemplate='%{label}<br>$%{value:,.0f}<br>%{percent}<extra></extra>'
        )])

        # Add center annotation
        fig_pie.add_annotation(
            text=f"<b>{format_currency(adult_total + medical_total)}</b><br><span style='font-size:12px;color:#8892b0'>YTD Total</span>",
            x=0.5, y=0.5, showarrow=False,
            font=dict(size=20, color='#ccd6f6')
        )

        fig_pie = apply_dark_theme(fig_pie, height=350,
            margin=dict(l=20, r=20, t=20, b=20),
            showlegend=True,
            legend={'orientation': 'h', 'yanchor': 'bottom', 'y': -0.1, 'xanchor': 'center', 'x': 0.5}
        )

        st.plotly_chart(fig_pie, use_container_width=True)

    with col_right:
        st.markdown('<div class="section-header">THC Beverage Market Share</div>', unsafe_allow_html=True)
        st.markdown('<span class="data-badge">H2 2024 Distributor Data</span>', unsafe_allow_html=True)

        if not beverage_df.empty:
            total_containers = beverage_df['containers_sold'].sum()

            # Show top distributors as styled bars
            for idx, row in beverage_df.head(6).iterrows():
                pct = (row['containers_sold'] / total_containers * 100) if total_containers > 0 else 0
                name = row['distributor_name'][:35] + '...' if len(row['distributor_name']) > 35 else row['distributor_name']

                st.markdown(f"""
                <div class="market-bar-container">
                    <div class="market-bar-label">
                        <span>{name}</span>
                        <span style="color: #00f5a0;">{format_number(row['containers_sold'])} containers</span>
                    </div>
                    <div class="market-bar">
                        <div class="market-bar-fill" style="width: {min(pct, 100)}%;"></div>
                    </div>
                </div>
                """, unsafe_allow_html=True)
        else:
            st.info("No THC beverage data available")

    # ==========================================
    # SEASONALITY ANALYSIS
    # ==========================================
    st.markdown('<div class="section-header">Seasonality & Product Volume Trends</div>', unsafe_allow_html=True)

    col1, col2 = st.columns(2)

    with col1:
        # Monthly aggregation for seasonality
        weekly_df['month'] = pd.to_datetime(weekly_df['week_date']).dt.strftime('%b')
        weekly_df['month_num'] = pd.to_datetime(weekly_df['week_date']).dt.month

        monthly_avg = weekly_df.groupby(['month_num', 'month']).agg({
            'total_sales': 'mean',
            'total_products_sold': 'mean'
        }).reset_index().sort_values('month_num')

        fig_season = go.Figure()

        fig_season.add_trace(go.Bar(
            x=monthly_avg['month'],
            y=monthly_avg['total_sales'] / 1_000_000,
            marker=dict(
                color=monthly_avg['total_sales'],
                colorscale=[[0, '#764ba2'], [0.5, '#667eea'], [1, '#00f5a0']],
                line=dict(width=0)
            ),
            hovertemplate='%{x}<br>$%{y:.2f}M avg/week<extra></extra>'
        ))

        fig_season = apply_dark_theme(fig_season, height=350,
            yaxis={'title': 'Avg Weekly Sales ($M)', 'tickprefix': '$', 'ticksuffix': 'M'},
            xaxis={'title': ''},
            title={'text': 'Average Weekly Sales by Month', 'font': {'size': 14, 'color': '#ccd6f6'}}
        )

        st.plotly_chart(fig_season, use_container_width=True)

    with col2:
        # Products sold trend
        fig_products = go.Figure()

        fig_products.add_trace(go.Scatter(
            x=weekly_df['week_date'],
            y=weekly_df['adult_use_products_sold'],
            fill='tozeroy',
            fillcolor='rgba(0, 245, 160, 0.15)',
            line=dict(color='#00f5a0', width=2),
            name='Adult Use Products',
            hovertemplate='%{y:,.0f}<extra></extra>'
        ))

        fig_products.add_trace(go.Scatter(
            x=weekly_df['week_date'],
            y=weekly_df['medical_products_sold'],
            fill='tozeroy',
            fillcolor='rgba(118, 75, 162, 0.15)',
            line=dict(color='#764ba2', width=2),
            name='Medical Products',
            hovertemplate='%{y:,.0f}<extra></extra>'
        ))

        fig_products = apply_dark_theme(fig_products, height=350,
            yaxis={'title': 'Products Sold'},
            xaxis={'title': ''},
            title={'text': 'Weekly Product Volume', 'font': {'size': 14, 'color': '#ccd6f6'}},
            legend={'orientation': 'h', 'yanchor': 'bottom', 'y': 1.02, 'xanchor': 'right', 'x': 1}
        )

        st.plotly_chart(fig_products, use_container_width=True)

    # ==========================================
    # DATA TABLE
    # ==========================================
    with st.expander("📊 View Raw Data", expanded=False):
        st.markdown("**Weekly Cannabis Sales Data (CT)**")
        display_df = weekly_df[['week_date', 'total_sales', 'adult_use_sales', 'medical_sales',
                                'total_products_sold', 'avg_adult_price']].copy()
        display_df.columns = ['Week', 'Total Sales', 'Adult Use', 'Medical', 'Products Sold', 'Avg Price']
        display_df = display_df.sort_values('Week', ascending=False)
        display_df['Total Sales'] = display_df['Total Sales'].apply(lambda x: f"${x:,.0f}")
        display_df['Adult Use'] = display_df['Adult Use'].apply(lambda x: f"${x:,.0f}")
        display_df['Medical'] = display_df['Medical'].apply(lambda x: f"${x:,.0f}")
        display_df['Avg Price'] = display_df['Avg Price'].apply(lambda x: f"${x:.2f}" if pd.notna(x) else "N/A")
        st.dataframe(display_df.head(20), use_container_width=True, hide_index=True)

        if not beverage_df.empty:
            st.markdown("**THC Beverage Sales by Distributor (CT)**")
            st.dataframe(beverage_df, use_container_width=True, hide_index=True)

    # Footer
    st.markdown("""
    <div style="text-align: center; margin-top: 48px; padding: 24px; color: #5a6785; font-size: 0.75rem;">
        <p>Data Source: Connecticut Open Data Portal (data.ct.gov)</p>
        <p>Datasets: Weekly Cannabis Sales (ucaf-96h6) • THC Beverage Sales (ujsp-7e48)</p>
        <p>Last refresh: Weekly via load_market_data.sh</p>
    </div>
    """, unsafe_allow_html=True)


if __name__ == "__main__":
    main()
