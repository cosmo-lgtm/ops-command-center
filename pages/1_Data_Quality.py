"""
Data Quality Command Center
VIP ↔ Salesforce Alignment • Data Quality Metrics
"""

import streamlit as st
import pandas as pd
from google.cloud import bigquery
from datetime import datetime
import plotly.graph_objects as go

# Page config - MUST be first Streamlit command
st.set_page_config(
    page_title="Data Quality",
    page_icon="🔬",
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
    .metric-value-red { font-size: clamp(1.5rem, 4vw, 2.25rem); font-weight: 700; background: linear-gradient(135deg, #ff6b6b 0%, #ee5a24 100%); -webkit-background-clip: text; -webkit-text-fill-color: transparent; margin: 0; }
    .metric-value-yellow { font-size: clamp(1.5rem, 4vw, 2.25rem); font-weight: 700; background: linear-gradient(135deg, #ffd666 0%, #f39c12 100%); -webkit-background-clip: text; -webkit-text-fill-color: transparent; margin: 0; }
    .metric-label { font-size: clamp(0.7rem, 1.5vw, 0.875rem); color: #8892b0; text-transform: uppercase; letter-spacing: 1px; margin-top: 6px; }
    .metric-sublabel { font-size: clamp(0.65rem, 1.2vw, 0.75rem); color: #5a6785; margin-top: 4px; }

    .dashboard-header { background: linear-gradient(90deg, #667eea 0%, #764ba2 100%); -webkit-background-clip: text; -webkit-text-fill-color: transparent; font-size: clamp(1.75rem, 5vw, 2.625rem); font-weight: 800; margin-bottom: 8px; }
    .dashboard-subtitle { color: #8892b0; font-size: clamp(0.875rem, 2vw, 1rem); margin-bottom: 24px; }
    .section-header { color: #ccd6f6; font-size: clamp(1.1rem, 2.5vw, 1.375rem); font-weight: 600; margin: 24px 0 12px 0; padding-bottom: 8px; border-bottom: 2px solid rgba(102, 126, 234, 0.3); }

    /* Status badges - responsive */
    .status-healthy { background: rgba(100, 255, 218, 0.2); color: #64ffda; padding: 3px 10px; border-radius: 20px; font-size: clamp(0.65rem, 1.2vw, 0.75rem); font-weight: 600; }
    .status-warning { background: rgba(255, 214, 102, 0.2); color: #ffd666; padding: 3px 10px; border-radius: 20px; font-size: clamp(0.65rem, 1.2vw, 0.75rem); font-weight: 600; }
    .status-critical { background: rgba(255, 107, 107, 0.2); color: #ff6b6b; padding: 3px 10px; border-radius: 20px; font-size: clamp(0.65rem, 1.2vw, 0.75rem); font-weight: 600; }

    .live-indicator { display: inline-flex; align-items: center; gap: 8px; color: #64ffda; font-size: clamp(0.65rem, 1.5vw, 0.75rem); text-transform: uppercase; letter-spacing: 1px; }
    .live-dot { width: 8px; height: 8px; background: #64ffda; border-radius: 50%; animation: pulse 2s infinite; }
    @keyframes pulse { 0%, 100% { opacity: 1; transform: scale(1); } 50% { opacity: 0.5; transform: scale(1.2); } }

    .alignment-row { display: flex; justify-content: space-between; align-items: center; padding: 12px 16px; background: linear-gradient(145deg, #1e1e2f 0%, #252540 100%); border-radius: 8px; margin-bottom: 8px; border: 1px solid rgba(255,255,255,0.05); }
    .alignment-label { color: #ccd6f6; font-weight: 600; font-size: clamp(0.75rem, 1.5vw, 0.875rem); }
    .alignment-values { display: flex; gap: 24px; align-items: center; }
    .alignment-value { text-align: center; }
    .alignment-number { font-size: clamp(0.95rem, 2vw, 1.125rem); font-weight: 700; color: #ccd6f6; }
    .alignment-source { font-size: clamp(0.6rem, 1vw, 0.625rem); color: #5a6785; text-transform: uppercase; }
    .delta-positive { color: #64ffda; font-size: clamp(0.65rem, 1.2vw, 0.75rem); }
    .delta-negative { color: #ff6b6b; font-size: clamp(0.65rem, 1.2vw, 0.75rem); }

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

COLORS = {'primary': '#667eea', 'secondary': '#764ba2', 'success': '#64ffda', 'warning': '#ffd666', 'danger': '#ff6b6b', 'info': '#74b9ff'}


def apply_dark_theme(fig, height=350, **kwargs):
    layout_args = {
        'paper_bgcolor': 'rgba(0,0,0,0)', 'plot_bgcolor': 'rgba(0,0,0,0)',
        'font': {'color': '#ccd6f6', 'family': 'Inter, sans-serif'}, 'height': height,
        'margin': kwargs.get('margin', dict(l=0, r=0, t=20, b=0)),
        'xaxis': {'gridcolor': 'rgba(255,255,255,0.1)', 'linecolor': 'rgba(255,255,255,0.1)', 'tickfont': {'color': '#8892b0'}, **kwargs.get('xaxis', {})},
        'yaxis': {'gridcolor': 'rgba(255,255,255,0.1)', 'linecolor': 'rgba(255,255,255,0.1)', 'tickfont': {'color': '#8892b0'}, **kwargs.get('yaxis', {})}
    }
    for k, v in kwargs.items():
        if k not in ['xaxis', 'yaxis', 'margin']:
            layout_args[k] = v
    fig.update_layout(**layout_args)
    return fig


@st.cache_resource
def get_bq_client():
    if "gcp_service_account" in st.secrets:
        from google.oauth2 import service_account
        credentials = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])
        return bigquery.Client(project='artful-logic-475116-p1', credentials=credentials)
    return bigquery.Client(project='artful-logic-475116-p1')


@st.cache_data(ttl=300)
def load_vip_match_quality():
    client = get_bq_client()
    return client.query("SELECT * FROM `artful-logic-475116-p1.staging_data_quality.vip_match_quality`").to_dataframe().iloc[0]


@st.cache_data(ttl=300)
def load_salesforce_quality():
    client = get_bq_client()
    return client.query("SELECT * FROM `artful-logic-475116-p1.staging_data_quality.salesforce_quality`").to_dataframe().iloc[0]


@st.cache_data(ttl=300)
def load_vip_sf_alignment():
    client = get_bq_client()
    return client.query("SELECT * FROM `artful-logic-475116-p1.staging_data_quality.vip_sf_alignment`").to_dataframe().iloc[0]


@st.cache_data(ttl=300)
def load_covered_doors_stats():
    """Load stats from the deduped active customer fact sheet"""
    client = get_bq_client()
    query = """
    SELECT
        COUNT(*) as total_covered_doors,
        SUM(CASE WHEN sfdc_account_id IS NOT NULL THEN 1 ELSE 0 END) as matched_to_sfdc,
        SUM(CASE WHEN vip_code_count > 1 THEN 1 ELSE 0 END) as consolidated_addresses,
        SUM(vip_code_count) - COUNT(*) as codes_consolidated,
        MAX(most_recent_order_date) as latest_depletion_date,
        SUM(qty_last_30_days) as total_30d_volume,
        SUM(CASE WHEN customer_status = 'Active' THEN 1 ELSE 0 END) as active_customers,
        SUM(CASE WHEN customer_status = 'At Risk' THEN 1 ELSE 0 END) as at_risk_customers,
        SUM(CASE WHEN customer_status = 'Churned' THEN 1 ELSE 0 END) as churned_customers
    FROM `artful-logic-475116-p1.staging_vip.retail_customer_fact_sheet_v2_deduped`
    """
    return client.query(query).to_dataframe().iloc[0]


def render_metric_card(value, label, sublabel=None, status="neutral"):
    value_class = {"healthy": "metric-value-green", "warning": "metric-value-yellow", "critical": "metric-value-red", "neutral": "metric-value"}.get(status, "metric-value")
    sublabel_html = f'<div class="metric-sublabel">{sublabel}</div>' if sublabel else ""
    return f'<div class="metric-card"><div class="{value_class}">{value}</div><div class="metric-label">{label}</div>{sublabel_html}</div>'


def render_alignment_row(label, vip_count, sf_count, matched_count, match_rate):
    delta = sf_count - vip_count
    delta_class = "delta-positive" if delta >= 0 else "delta-negative"
    delta_sign = "+" if delta >= 0 else ""
    rate_class = "status-healthy" if match_rate >= 90 else "status-warning" if match_rate >= 70 else "status-critical"
    return f'''<div class="alignment-row"><div class="alignment-label">{label}</div><div class="alignment-values">
        <div class="alignment-value"><div class="alignment-number">{vip_count:,}</div><div class="alignment-source">VIP</div></div>
        <div class="alignment-value"><div class="alignment-number">{sf_count:,}</div><div class="alignment-source">Salesforce</div></div>
        <div class="alignment-value"><div class="alignment-number">{matched_count:,}</div><div class="alignment-source">Matched</div></div>
        <div class="alignment-value"><div class="{delta_class}">{delta_sign}{delta:,}</div><div class="alignment-source">Delta</div></div>
        <div><span class="{rate_class}">{match_rate:.0f}%</span></div></div></div>'''


def calculate_health_score(vip_stats, sf_stats, alignment_stats):
    scores = []
    if vip_stats is not None:
        scores.append((vip_stats.get('match_rate_pct', 0) or 0) / 100 * 35)
    else:
        scores.append(0)
    if alignment_stats is not None:
        avg_alignment = ((alignment_stats.get('retail_match_rate_pct', 0) or 0) + (alignment_stats.get('distributor_match_rate_pct', 0) or 0) + (alignment_stats.get('chain_match_rate_pct', 0) or 0)) / 3
        scores.append(avg_alignment / 100 * 40)
    else:
        scores.append(0)
    if sf_stats is not None:
        sf_score = (((sf_stats.get('account_name_completeness', 100) or 100) + (sf_stats.get('phone_completeness', 0) or 0)) / 200 * 25) - min(10, (sf_stats.get('accounts_with_duplicate_names', 0) or 0) / 1000)
        scores.append(max(0, sf_score))
    else:
        scores.append(0)
    return round(sum(scores))


def main():
    try:
        vip_stats = load_vip_match_quality()
        sf_stats = load_salesforce_quality()
        alignment_stats = load_vip_sf_alignment()
        covered_doors = load_covered_doors_stats()
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return

    # Calculate VIP data freshness
    latest_depletion = covered_doors['latest_depletion_date']
    if pd.notna(latest_depletion):
        if isinstance(latest_depletion, str):
            latest_depletion = datetime.strptime(latest_depletion, '%Y-%m-%d').date()
        days_old = (datetime.now().date() - latest_depletion).days
        freshness_status = "healthy" if days_old <= 7 else "warning" if days_old <= 14 else "critical"
        freshness_text = f"VIP Data: {latest_depletion.strftime('%b %d')} ({days_old}d old)"
    else:
        freshness_status = "critical"
        freshness_text = "VIP Data: Unknown"

    freshness_class = {"healthy": "status-healthy", "warning": "status-warning", "critical": "status-critical"}[freshness_status]

    st.markdown(f'''<div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 32px;">
        <div><h1 class="dashboard-header">Data Quality Command Center</h1><p class="dashboard-subtitle">VIP ↔ Salesforce Alignment • Data Quality Metrics</p></div>
        <div style="display: flex; align-items: center; gap: 16px;">
            <span class="{freshness_class}">{freshness_text}</span>
            <div class="live-indicator"><span class="live-dot"></span>Live Data</div>
        </div></div>''', unsafe_allow_html=True)

    health_score = calculate_health_score(vip_stats, sf_stats, alignment_stats)
    health_status = "healthy" if health_score >= 80 else "warning" if health_score >= 60 else "critical"

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.markdown(render_metric_card(f"{health_score}", "Health Score", "System-wide quality", health_status), unsafe_allow_html=True)
    with col2:
        match_rate = alignment_stats['retail_match_rate_pct']
        st.markdown(render_metric_card(f"{match_rate:.1f}%", "Retail Match Rate", f"{alignment_stats['matched_retail_count']:,} matched", "healthy" if match_rate >= 90 else "warning" if match_rate >= 75 else "critical"), unsafe_allow_html=True)
    with col3:
        dist_rate = alignment_stats['distributor_match_rate_pct']
        st.markdown(render_metric_card(f"{dist_rate:.1f}%", "Distributor Match", f"{alignment_stats['matched_distributor_count']:,} matched", "healthy" if dist_rate >= 90 else "warning" if dist_rate >= 75 else "critical"), unsafe_allow_html=True)
    with col4:
        dup_count = sf_stats['accounts_with_duplicate_names']
        st.markdown(render_metric_card(f"{dup_count:,}", "Duplicate Names", "Salesforce Accounts", "healthy" if dup_count < 1000 else "warning" if dup_count < 5000 else "critical"), unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown('<p class="section-header">VIP ↔ Salesforce Alignment (Full Universe)</p>', unsafe_allow_html=True)
    st.markdown(render_alignment_row("Retail Locations", alignment_stats['vip_retail_count'], alignment_stats['sf_retail_count'], alignment_stats['matched_retail_count'], alignment_stats['retail_match_rate_pct']), unsafe_allow_html=True)
    st.markdown(render_alignment_row("Distributors", alignment_stats['vip_distributor_count'], alignment_stats['sf_distributor_count'], alignment_stats['matched_distributor_count'], alignment_stats['distributor_match_rate_pct']), unsafe_allow_html=True)
    st.markdown(render_alignment_row("Chain HQs", alignment_stats['vip_chain_count'], alignment_stats['sf_chain_hq_count'], alignment_stats['matched_chain_count'], alignment_stats['chain_match_rate_pct']), unsafe_allow_html=True)

    # Covered Doors Section (Active Customers - Deduped)
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown('<p class="section-header">Covered Doors (Active Customers)</p>', unsafe_allow_html=True)

    cd_col1, cd_col2, cd_col3, cd_col4 = st.columns(4)
    with cd_col1:
        total_doors = int(covered_doors['total_covered_doors'])
        st.markdown(render_metric_card(f"{total_doors:,}", "Unique Doors", "Deduped by address", "neutral"), unsafe_allow_html=True)
    with cd_col2:
        matched = int(covered_doors['matched_to_sfdc'])
        match_pct = 100 * matched / total_doors if total_doors > 0 else 0
        st.markdown(render_metric_card(f"{match_pct:.1f}%", "SFDC Match Rate", f"{matched:,} matched", "healthy" if match_pct >= 95 else "warning" if match_pct >= 85 else "critical"), unsafe_allow_html=True)
    with cd_col3:
        active = int(covered_doors['active_customers'])
        active_pct = 100 * active / total_doors if total_doors > 0 else 0
        st.markdown(render_metric_card(f"{active:,}", "Active (30d)", f"{active_pct:.0f}% of doors", "healthy" if active_pct >= 50 else "warning" if active_pct >= 30 else "neutral"), unsafe_allow_html=True)
    with cd_col4:
        consolidated = int(covered_doors['consolidated_addresses'])
        codes_merged = int(covered_doors['codes_consolidated'])
        st.markdown(render_metric_card(f"{consolidated}", "Consolidated", f"{codes_merged} VIP codes merged", "neutral"), unsafe_allow_html=True)

    # Customer status breakdown
    cd_sub1, cd_sub2, cd_sub3 = st.columns(3)
    with cd_sub1:
        at_risk = int(covered_doors['at_risk_customers'])
        st.markdown(render_metric_card(f"{at_risk:,}", "At Risk (31-90d)", status="warning"), unsafe_allow_html=True)
    with cd_sub2:
        churned = int(covered_doors['churned_customers'])
        st.markdown(render_metric_card(f"{churned:,}", "Churned (>90d)", status="critical"), unsafe_allow_html=True)
    with cd_sub3:
        vol_30d = int(covered_doors['total_30d_volume'])
        st.markdown(render_metric_card(f"{vol_30d:,}", "30-Day Volume", "Total units", "neutral"), unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    col1, col2 = st.columns(2)

    with col1:
        st.markdown('<p class="section-header">VIP Data Quality</p>', unsafe_allow_html=True)
        subcol1, subcol2, subcol3 = st.columns(3)
        with subcol1:
            st.markdown(render_metric_card(f"{vip_stats['total_vip_accounts']:,}", "Total VIP Accounts", status="neutral"), unsafe_allow_html=True)
        with subcol2:
            chain_coverage = vip_stats['chain_hq_coverage_pct']
            st.markdown(render_metric_card(f"{chain_coverage:.0f}%", "Chain HQ Coverage", f"{vip_stats['chains_with_hq']}/{vip_stats['total_chains']} chains", "healthy" if chain_coverage >= 70 else "warning" if chain_coverage >= 50 else "critical"), unsafe_allow_html=True)
        with subcol3:
            dist_rate = vip_stats['distributor_match_rate_pct']
            st.markdown(render_metric_card(f"{dist_rate:.0f}%", "Distributor Match", f"{vip_stats['distributors_matched_sf']}/{vip_stats['active_distributors']}", "healthy" if dist_rate >= 90 else "warning" if dist_rate >= 70 else "critical"), unsafe_allow_html=True)

        match_data = pd.DataFrame({'Status': ['Matched', 'Unmatched'], 'Count': [vip_stats['matched_to_sf'], vip_stats['unmatched']]})
        fig = go.Figure(data=[go.Pie(labels=match_data['Status'], values=match_data['Count'], hole=0.6, marker_colors=[COLORS['success'], COLORS['danger']], textinfo='percent', textfont=dict(color='white'))])
        apply_dark_theme(fig, height=200, margin=dict(l=20, r=20, t=20, b=20), showlegend=True, legend=dict(orientation="h", yanchor="bottom", y=-0.2, xanchor="center", x=0.5, font=dict(color='#8892b0')))
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown('<p class="section-header">Salesforce Data Quality</p>', unsafe_allow_html=True)
        subcol1, subcol2, subcol3 = st.columns(3)
        with subcol1:
            st.markdown(render_metric_card(f"{sf_stats['total_accounts']:,}", "Total Accounts", status="neutral"), unsafe_allow_html=True)
        with subcol2:
            vip_coverage = sf_stats['vip_coverage_pct']
            st.markdown(render_metric_card(f"{vip_coverage:.0f}%", "VIP Coverage", f"{sf_stats['accounts_with_vip_id']:,} with VIP ID", "healthy" if vip_coverage >= 70 else "warning" if vip_coverage >= 50 else "critical"), unsafe_allow_html=True)
        with subcol3:
            active_rate = sf_stats['active_rate_pct']
            st.markdown(render_metric_card(f"{active_rate:.1f}%", "Active (90d)", f"{sf_stats['active_last_90d']:,} accounts", "warning" if active_rate < 5 else "neutral"), unsafe_allow_html=True)

        completeness_data = pd.DataFrame({'Field': ['Name', 'Address', 'Phone', 'Email (Contacts)'], 'Completeness': [sf_stats['account_name_completeness'], sf_stats['address_completeness'], sf_stats['phone_completeness'], sf_stats['contact_email_completeness']]})
        fig = go.Figure(go.Bar(x=completeness_data['Completeness'], y=completeness_data['Field'], orientation='h', marker=dict(color=completeness_data['Completeness'], colorscale=[[0, COLORS['danger']], [0.5, COLORS['warning']], [1, COLORS['success']]], cmin=0, cmax=100), hovertemplate='%{y}: %{x:.1f}%<extra></extra>'))
        apply_dark_theme(fig, height=200, margin=dict(l=0, r=20, t=10, b=10), xaxis={'range': [0, 100]})
        st.plotly_chart(fig, use_container_width=True)

    st.markdown(f'''<div style="text-align: center; color: #8892b0; margin-top: 48px; padding: 24px; border-top: 1px solid rgba(255,255,255,0.1);">
        <p style="margin: 0;">Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} UTC</p>
        <p style="margin: 4px 0 0 0; font-size: 12px;">Data refreshes every 5 minutes</p></div>''', unsafe_allow_html=True)


if __name__ == "__main__":
    main()
