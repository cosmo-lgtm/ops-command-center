"""
Authorize.net Payments Dashboard
Payment analytics from BigQuery (synced from Authorize.net).
Shows payments by amount, card type, and geography.
"""

import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta
import plotly.express as px

# Page config
st.set_page_config(
    page_title="Payments Dashboard",
    page_icon="💳",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Dark mode CSS (matching ops-command-center theme)
st.markdown("""
<style>
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

    /* KPI Cards */
    .kpi-card {
        background: linear-gradient(145deg, #1e1e2f 0%, #2a2a4a 100%);
        border-radius: 12px;
        padding: 20px;
        border: 1px solid rgba(255,255,255,0.1);
        text-align: center;
    }
    .kpi-value {
        font-size: 32px;
        font-weight: 700;
        color: #00d4aa;
        margin: 0;
    }
    .kpi-label {
        font-size: 12px;
        color: #8892b0;
        text-transform: uppercase;
        letter-spacing: 1px;
        margin-top: 4px;
    }
    .kpi-delta-positive {
        color: #64ffda;
        font-size: 12px;
    }
    .kpi-delta-negative {
        color: #ff6b6b;
        font-size: 12px;
    }
</style>
""", unsafe_allow_html=True)

COLORS = {
    'primary': '#00d4aa',
    'secondary': '#667eea',
    'visa': '#1A1F71',
    'mastercard': '#EB001B',
    'amex': '#006FCF',
    'discover': '#FF6000',
    'other': '#8892b0',
}


# BigQuery connection
@st.cache_resource
def get_bq_client():
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"]
    )
    return bigquery.Client(credentials=credentials, project="artful-logic-475116-p1")


@st.cache_data(ttl=300)
def run_query(query: str) -> pd.DataFrame:
    client = get_bq_client()
    return client.query(query).to_dataframe()


@st.cache_data(ttl=300)
def load_payments(start_date: str, end_date: str) -> pd.DataFrame:
    """Load payment transactions from BigQuery."""
    return run_query(f"""
    SELECT
        transaction_id,
        settlement_date,
        DATE(settlement_date) as date,
        amount,
        card_type,
        card_last4,
        CONCAT(first_name, ' ', last_name) as customer_name,
        company,
        city,
        UPPER(TRIM(state)) as state,
        zip,
        COALESCE(UPPER(TRIM(country)), 'US') as country,
        invoice_number,
        CASE
            WHEN UPPER(TRIM(state)) IN ('CT', 'ME', 'MA', 'NH', 'RI', 'VT', 'NJ', 'NY', 'PA') THEN 'Northeast'
            WHEN UPPER(TRIM(state)) IN ('IL', 'IN', 'MI', 'OH', 'WI', 'IA', 'KS', 'MN', 'MO', 'NE', 'ND', 'SD') THEN 'Midwest'
            WHEN UPPER(TRIM(state)) IN ('DE', 'FL', 'GA', 'MD', 'NC', 'SC', 'VA', 'DC', 'WV', 'AL', 'KY', 'MS', 'TN', 'AR', 'LA', 'OK', 'TX') THEN 'South'
            WHEN UPPER(TRIM(state)) IN ('AZ', 'CO', 'ID', 'MT', 'NV', 'NM', 'UT', 'WY', 'AK', 'CA', 'HI', 'OR', 'WA') THEN 'West'
            ELSE 'Other'
        END as region
    FROM `artful-logic-475116-p1.raw_authnet.transactions`
    WHERE status = 'settledSuccessfully'
        AND DATE(settlement_date) >= '{start_date}'
        AND DATE(settlement_date) <= '{end_date}'
    ORDER BY settlement_date DESC
    """)


@st.cache_data(ttl=300)
def load_daily_summary(start_date: str, end_date: str) -> pd.DataFrame:
    """Load daily payment summary."""
    return run_query(f"""
    SELECT
        DATE(settlement_date) as date,
        COUNT(*) as transaction_count,
        SUM(amount) as revenue,
        COUNT(DISTINCT card_type) as card_types
    FROM `artful-logic-475116-p1.raw_authnet.transactions`
    WHERE status = 'settledSuccessfully'
        AND DATE(settlement_date) >= '{start_date}'
        AND DATE(settlement_date) <= '{end_date}'
    GROUP BY date
    ORDER BY date
    """)


@st.cache_data(ttl=300)
def load_state_summary(start_date: str, end_date: str) -> pd.DataFrame:
    """Load state-level summary."""
    return run_query(f"""
    SELECT
        UPPER(TRIM(state)) as state,
        COALESCE(UPPER(TRIM(country)), 'US') as country,
        COUNT(*) as transaction_count,
        SUM(amount) as revenue
    FROM `artful-logic-475116-p1.raw_authnet.transactions`
    WHERE status = 'settledSuccessfully'
        AND DATE(settlement_date) >= '{start_date}'
        AND DATE(settlement_date) <= '{end_date}'
    GROUP BY state, country
    ORDER BY revenue DESC
    """)


@st.cache_data(ttl=300)
def load_card_summary(start_date: str, end_date: str) -> pd.DataFrame:
    """Load card type summary."""
    return run_query(f"""
    SELECT
        card_type,
        COUNT(*) as transaction_count,
        SUM(amount) as revenue
    FROM `artful-logic-475116-p1.raw_authnet.transactions`
    WHERE status = 'settledSuccessfully'
        AND DATE(settlement_date) >= '{start_date}'
        AND DATE(settlement_date) <= '{end_date}'
    GROUP BY card_type
    ORDER BY revenue DESC
    """)


def render_kpi_card(value: str, label: str, delta: str = None, delta_positive: bool = True):
    """Render a KPI card."""
    delta_html = ""
    if delta:
        delta_class = "kpi-delta-positive" if delta_positive else "kpi-delta-negative"
        delta_html = f'<div class="{delta_class}">{delta}</div>'

    return f"""
    <div class="kpi-card">
        <div class="kpi-value">{value}</div>
        <div class="kpi-label">{label}</div>
        {delta_html}
    </div>
    """


def main():
    st.markdown("""
    <h1 style="color: #ccd6f6; margin-bottom: 8px;">💳 Payments Dashboard</h1>
    <p style="color: #8892b0; margin-bottom: 24px;">Authorize.net transaction analytics</p>
    """, unsafe_allow_html=True)

    # Sidebar filters
    st.sidebar.markdown("### Filters")

    # Date range
    today = datetime.now().date()
    default_start = today - timedelta(days=30)

    date_range = st.sidebar.date_input(
        "Date Range",
        value=(default_start, today),
        max_value=today
    )

    if len(date_range) == 2:
        start_date, end_date = date_range
    else:
        start_date = end_date = date_range[0]

    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    # Load data
    try:
        df = load_payments(start_str, end_str)
        daily_df = load_daily_summary(start_str, end_str)
        state_df = load_state_summary(start_str, end_str)
        card_df = load_card_summary(start_str, end_str)
    except Exception as e:
        st.error(f"""
        **Could not load payment data from BigQuery.**

        Error: {str(e)}

        Make sure the `raw_authnet.transactions` table exists and has been synced.
        Run the sync script: `python integrations/authorize-net/sync_to_bigquery.py`
        """)
        return

    if df.empty:
        st.warning("No transactions found for the selected date range.")
        return

    # Filter by country
    countries = ["All"] + sorted(df['country'].dropna().unique().tolist())
    selected_country = st.sidebar.selectbox("Country", countries)

    if selected_country != "All":
        df = df[df['country'] == selected_country]
        state_df = state_df[state_df['country'] == selected_country]

    # KPI Cards
    total_revenue = df['amount'].sum()
    transaction_count = len(df)
    avg_transaction = df['amount'].mean() if transaction_count > 0 else 0

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.markdown(render_kpi_card(
            f"${total_revenue:,.2f}",
            "Total Revenue"
        ), unsafe_allow_html=True)

    with col2:
        st.markdown(render_kpi_card(
            f"{transaction_count:,}",
            "Transactions"
        ), unsafe_allow_html=True)

    with col3:
        st.markdown(render_kpi_card(
            f"${avg_transaction:,.2f}",
            "Avg Transaction"
        ), unsafe_allow_html=True)

    with col4:
        us_states = state_df[state_df['country'] == 'US']['state'].nunique()
        st.markdown(render_kpi_card(
            f"{us_states}",
            "US States"
        ), unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # Charts Row 1
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### Payments Over Time")
        if not daily_df.empty:
            fig = px.bar(
                daily_df,
                x='date',
                y='revenue',
                labels={'date': 'Date', 'revenue': 'Revenue ($)'},
                color_discrete_sequence=[COLORS['primary']]
            )
            fig.update_layout(
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                font_color='#ccd6f6',
                xaxis=dict(gridcolor='rgba(255,255,255,0.1)'),
                yaxis=dict(gridcolor='rgba(255,255,255,0.1)')
            )
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown("### By Card Type")
        if not card_df.empty:
            color_map = {
                'Visa': COLORS['visa'],
                'MasterCard': COLORS['mastercard'],
                'Mastercard': COLORS['mastercard'],
                'AmericanExpress': COLORS['amex'],
                'Amex': COLORS['amex'],
                'Discover': COLORS['discover'],
            }

            fig = px.pie(
                card_df,
                values='revenue',
                names='card_type',
                color='card_type',
                color_discrete_map=color_map,
                hole=0.4
            )
            fig.update_layout(
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                font_color='#ccd6f6',
                legend=dict(orientation="h", yanchor="bottom", y=-0.2)
            )
            st.plotly_chart(fig, use_container_width=True)

    # Charts Row 2 - Geography
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### US States")
        us_state_df = state_df[state_df['country'] == 'US'].copy()

        if not us_state_df.empty:
            fig = px.choropleth(
                us_state_df,
                locations='state',
                locationmode='USA-states',
                color='revenue',
                scope='usa',
                color_continuous_scale='Viridis',
                labels={'revenue': 'Revenue ($)'}
            )
            fig.update_layout(
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                font_color='#ccd6f6',
                geo=dict(
                    bgcolor='rgba(0,0,0,0)',
                    lakecolor='rgba(0,0,0,0)',
                    landcolor='rgba(30,30,47,1)',
                )
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No US transactions in selected range.")

    with col2:
        st.markdown("### International")
        intl_df = state_df[state_df['country'] != 'US'].copy()

        if not intl_df.empty:
            country_agg = intl_df.groupby('country').agg({
                'revenue': 'sum',
                'transaction_count': 'sum'
            }).reset_index().sort_values('revenue', ascending=False).head(10)

            fig = px.bar(
                country_agg,
                x='revenue',
                y='country',
                orientation='h',
                labels={'revenue': 'Revenue ($)', 'country': 'Country'},
                color_discrete_sequence=[COLORS['secondary']]
            )
            fig.update_layout(
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                font_color='#ccd6f6',
                xaxis=dict(gridcolor='rgba(255,255,255,0.1)'),
                yaxis=dict(gridcolor='rgba(255,255,255,0.1)', categoryorder='total ascending')
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No international transactions in selected range.")

    # Top States Table
    st.markdown("### Top States by Revenue")
    us_top = us_state_df.head(10).copy()
    if not us_top.empty:
        us_top['revenue'] = us_top['revenue'].apply(lambda x: f"${x:,.2f}")
        us_top.columns = ['State', 'Country', 'Transactions', 'Revenue']
        st.dataframe(us_top[['State', 'Revenue', 'Transactions']], use_container_width=True, hide_index=True)

    # Recent Transactions
    st.markdown("### Recent Transactions")
    recent = df.head(20).copy()
    recent['amount'] = recent['amount'].apply(lambda x: f"${x:,.2f}")
    recent['settlement_date'] = pd.to_datetime(recent['settlement_date']).dt.strftime('%Y-%m-%d %H:%M')

    display_cols = ['settlement_date', 'amount', 'card_type', 'card_last4', 'city', 'state', 'country', 'invoice_number']
    display_df = recent[display_cols].copy()
    display_df.columns = ['Date/Time', 'Amount', 'Card', 'Last 4', 'City', 'State', 'Country', 'Invoice']

    st.dataframe(display_df, use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
