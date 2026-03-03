"""
Marketing Scorecard Dashboard
Weekly KPI scorecard for sales leadership meetings.
Automates Retail Revenue, DTC Revenue, Active Doors, LinkedIn & Instagram followers.
Manual metrics (rebates, influencer posts, events) pulled from Google Sheet.
"""

import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import plotly.graph_objects as go
from datetime import datetime, timedelta, date
import re
import requests
from bs4 import BeautifulSoup
import json

# Page config
st.set_page_config(
    page_title="Marketing Scorecard",
    page_icon="📋",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Dark mode CSS
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
        padding: 16px;
        border: 1px solid rgba(255,255,255,0.1);
        text-align: center;
        min-height: 160px;
    }
    .kpi-metric-name {
        font-size: 13px;
        color: #ccd6f6;
        font-weight: 600;
        margin: 0 0 2px 0;
    }
    .kpi-owner {
        font-size: 10px;
        color: #8892b0;
        text-transform: uppercase;
        letter-spacing: 1px;
        margin: 0 0 8px 0;
    }
    .kpi-value {
        font-size: 28px;
        font-weight: 700;
        color: #00d4aa;
        margin: 0;
    }
    .kpi-target {
        font-size: 11px;
        color: #8892b0;
        margin: 4px 0;
    }
    .kpi-pct {
        font-size: 14px;
        font-weight: 600;
        margin: 4px 0 6px 0;
    }
    .pct-green { color: #64ffda; }
    .pct-yellow { color: #ffd666; }
    .pct-red { color: #ff6b6b; }

    /* Progress bar */
    .progress-bg {
        background: rgba(255,255,255,0.1);
        border-radius: 4px;
        height: 6px;
        overflow: hidden;
    }
    .progress-fill-green {
        background: linear-gradient(90deg, #00d4aa, #64ffda);
        height: 100%;
        border-radius: 4px;
    }
    .progress-fill-yellow {
        background: linear-gradient(90deg, #ffa726, #ffd666);
        height: 100%;
        border-radius: 4px;
    }
    .progress-fill-red {
        background: linear-gradient(90deg, #e53935, #ff6b6b);
        height: 100%;
        border-radius: 4px;
    }

    /* Weekly tracker table */
    .tracker-table {
        width: 100%;
        border-collapse: collapse;
        font-size: 12px;
        color: #ccd6f6;
    }
    .tracker-table th {
        background: rgba(30, 30, 47, 0.8);
        padding: 8px 6px;
        text-align: center;
        font-weight: 600;
        color: #8892b0;
        font-size: 11px;
        border-bottom: 1px solid rgba(255,255,255,0.1);
        position: sticky;
        top: 0;
    }
    .tracker-table th.metric-col {
        text-align: left;
        min-width: 180px;
    }
    .tracker-table td {
        padding: 6px;
        text-align: center;
        border-bottom: 1px solid rgba(255,255,255,0.05);
    }
    .tracker-table td.metric-name {
        text-align: left;
        font-weight: 600;
        color: #ccd6f6;
    }
    .tracker-table td.metric-owner {
        text-align: left;
        color: #8892b0;
        font-size: 11px;
    }
    .tracker-table tr:hover {
        background: rgba(255,255,255,0.03);
    }
    .current-week {
        background: rgba(0, 212, 170, 0.1) !important;
        border-left: 2px solid #00d4aa;
        border-right: 2px solid #00d4aa;
    }
    .current-week-header {
        background: rgba(0, 212, 170, 0.2) !important;
        color: #00d4aa !important;
        border-left: 2px solid #00d4aa;
        border-right: 2px solid #00d4aa;
    }

    /* Section headers */
    .section-header {
        color: #ccd6f6;
        font-size: 20px;
        font-weight: 600;
        margin: 24px 0 12px 0;
        padding-bottom: 6px;
        border-bottom: 2px solid rgba(0, 212, 170, 0.3);
    }

    /* Source badge */
    .source-auto { color: #64ffda; font-size: 9px; }
    .source-sheet { color: #ffd666; font-size: 9px; }
</style>
""", unsafe_allow_html=True)

# ============================================================================
# CONFIGURATION
# ============================================================================

SHEET_ID = "182TSuNolIqE_76NMpVO9ey378eYg4pFDpAQ9t76z-yo"
BQ_PROJECT = "artful-logic-475116-p1"

# Week start dates (Monday-anchored) matching the Excel scorecard
WEEK_DATES = [
    date(2026, 1, 1), date(2026, 1, 5), date(2026, 1, 12), date(2026, 1, 19),
    date(2026, 1, 26), date(2026, 2, 2), date(2026, 2, 9), date(2026, 2, 16),
    date(2026, 2, 23), date(2026, 3, 2), date(2026, 3, 9), date(2026, 3, 16),
    date(2026, 3, 23), date(2026, 3, 30),
]

METRICS_CONFIG = [
    {"key": "retail_revenue", "name": "Retail Revenue", "owner": "ALL", "target": 9_000_000, "format": "currency", "source": "auto"},
    {"key": "dtc_revenue", "name": "DTC Revenue", "owner": "ALL", "target": 3_000_000, "format": "currency", "source": "auto"},
    {"key": "active_doors", "name": "Active Door Count", "owner": "ALL", "target": 9_000, "format": "number", "source": "auto"},
    {"key": "rebate_redemption", "name": "Redemption on H1 Rebate", "owner": "FRANK", "target": 9_128, "format": "currency", "source": "sheet"},
    {"key": "rebate_submissions", "name": "Submission of Redemptions", "owner": "FRANK", "target": 2_403, "format": "number", "source": "sheet"},
    {"key": "linkedin_followers", "name": "LinkedIn Followers", "owner": "SYDNEY", "target": 10_000, "format": "number", "source": "auto"},
    {"key": "instagram_followers", "name": "Instagram Followers", "owner": "SYDNEY", "target": 77_000, "format": "number", "source": "auto"},
    {"key": "influencer_posts", "name": "Black Cherry Influencer Box Posts", "owner": "SYDNEY", "target": 250, "format": "number", "source": "sheet"},
    {"key": "tentpole_events", "name": "Locking in Tentpole Events for NGP", "owner": "MARK", "target": 5, "format": "number", "source": "sheet"},
    {"key": "thc_events", "name": "Events/Partnerships for THC", "owner": "MARK", "target": 5, "format": "number", "source": "sheet"},
]


# ============================================================================
# DATA LOADING
# ============================================================================

@st.cache_resource
def get_bq_client():
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"]
    )
    return bigquery.Client(credentials=credentials, project=BQ_PROJECT)


@st.cache_data(ttl=300)
def run_query(query: str) -> pd.DataFrame:
    client = get_bq_client()
    return client.query(query).to_dataframe()


@st.cache_data(ttl=300)
def load_b2b_weekly_revenue() -> pd.DataFrame:
    """Weekly retail (B2B) revenue from Salesforce orders."""
    return run_query("""
    SELECT
        DATE_TRUNC(order_date, WEEK(MONDAY)) as week_start,
        ROUND(SUM(CAST(line_total_price AS FLOAT64)), 2) as revenue
    FROM `artful-logic-475116-p1.staging_salesforce.salesforce_orders_flattened`
    WHERE order_status != 'Draft'
        AND order_date >= '2026-01-01'
        AND order_date <= '2026-03-31'
    GROUP BY week_start
    ORDER BY week_start
    """)


@st.cache_data(ttl=300)
def load_b2c_weekly_revenue() -> pd.DataFrame:
    """Weekly DTC (B2C) net revenue from Shopify orders."""
    return run_query("""
    SELECT
        DATE_TRUNC(DATE(created_at), WEEK(MONDAY)) as week_start,
        ROUND(SUM(CAST(current_subtotal_price AS FLOAT64)), 2) as revenue
    FROM `artful-logic-475116-p1.raw_shopify.orders`
    WHERE cancelled_at IS NULL
        AND financial_status IN ('paid', 'partially_refunded')
        AND DATE(created_at) >= '2026-01-01'
        AND DATE(created_at) <= '2026-03-31'
    GROUP BY week_start
    ORDER BY week_start
    """)


@st.cache_data(ttl=300)
def load_active_door_count() -> int:
    """Current active door count from VIP retail fact sheet (ordered within 30 days)."""
    df = run_query("""
    SELECT COUNT(DISTINCT vip_id) as active_doors
    FROM `artful-logic-475116-p1.staging_vip.retail_customer_fact_sheet_2026`
    WHERE days_since_last_order <= 90
    """)
    return int(df['active_doors'].iloc[0]) if not df.empty else 0


@st.cache_data(ttl=3600)
def scrape_linkedin_followers() -> int | None:
    """Scrape LinkedIn company page for follower count."""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        resp = requests.get('https://www.linkedin.com/company/trynowadays/', headers=headers, timeout=10)
        if resp.status_code != 200:
            return None
        soup = BeautifulSoup(resp.text, 'html.parser')
        # LinkedIn embeds follower count in meta description or page content
        # Try meta description first
        meta = soup.find('meta', {'name': 'description'})
        if meta and meta.get('content'):
            match = re.search(r'([\d,]+)\s+followers', meta['content'])
            if match:
                return int(match.group(1).replace(',', ''))
        # Try page content
        text = soup.get_text()
        match = re.search(r'([\d,]+)\s+followers', text)
        if match:
            return int(match.group(1).replace(',', ''))
    except Exception:
        pass
    return None


@st.cache_data(ttl=3600)
def scrape_instagram_followers() -> int | None:
    """Scrape Instagram profile for follower count."""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        resp = requests.get('https://www.instagram.com/trynowadays/', headers=headers, timeout=10)
        if resp.status_code != 200:
            return None
        soup = BeautifulSoup(resp.text, 'html.parser')
        # Instagram puts follower count in og:description meta tag
        meta = soup.find('meta', {'property': 'og:description'})
        if meta and meta.get('content'):
            match = re.search(r'([\d,.]+[KMkm]?)\s+Followers', meta['content'])
            if match:
                raw = match.group(1).replace(',', '')
                if raw.upper().endswith('K'):
                    return int(float(raw[:-1]) * 1000)
                elif raw.upper().endswith('M'):
                    return int(float(raw[:-1]) * 1000000)
                return int(float(raw))
        # Try JSON-LD
        for script in soup.find_all('script', type='application/ld+json'):
            try:
                data = json.loads(script.string)
                if isinstance(data, dict) and 'mainEntityofPage' in data:
                    interaction = data.get('interactionStatistic', {})
                    if isinstance(interaction, dict):
                        return int(interaction.get('userInteractionCount', 0))
            except (json.JSONDecodeError, ValueError):
                continue
    except Exception:
        pass
    return None


@st.cache_data(ttl=300)
def load_google_sheet() -> pd.DataFrame | None:
    """Load manual metrics from Google Sheet."""
    try:
        import gspread
        from google.oauth2.service_account import Credentials

        scopes = [
            'https://www.googleapis.com/auth/spreadsheets.readonly',
            'https://www.googleapis.com/auth/drive.readonly',
        ]
        creds = Credentials.from_service_account_info(
            st.secrets["gcp_service_account"], scopes=scopes
        )
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(SHEET_ID)
        worksheet = sh.sheet1
        data = worksheet.get_all_values()
        if not data or len(data) < 2:
            return None
        df = pd.DataFrame(data[1:], columns=data[0])
        return df
    except Exception as e:
        st.sidebar.warning(f"Sheet load failed: {e}")
        return None


# ============================================================================
# FORMATTING HELPERS
# ============================================================================

def fmt_value(value, fmt_type: str) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "—"
    if fmt_type == "currency":
        v = float(value)
        if abs(v) >= 1_000_000:
            return f"${v/1_000_000:,.2f}M"
        elif abs(v) >= 1_000:
            return f"${v:,.0f}"
        else:
            return f"${v:,.2f}"
    else:
        return f"{int(float(value)):,}"


def fmt_cell(value, fmt_type: str) -> str:
    """Format a cell value for the weekly tracker."""
    if value is None or value == "" or (isinstance(value, float) and pd.isna(value)):
        return ""
    try:
        v = float(str(value).replace('$', '').replace(',', ''))
    except (ValueError, TypeError):
        return str(value)
    if fmt_type == "currency":
        if abs(v) >= 1_000_000:
            return f"${v/1_000_000:,.1f}M"
        elif abs(v) >= 1_000:
            return f"${v:,.0f}"
        else:
            return f"${v:,.0f}"
    else:
        return f"{int(v):,}"


def pct_class(pct: float) -> str:
    if pct >= 80:
        return "green"
    elif pct >= 50:
        return "yellow"
    else:
        return "red"


def render_kpi_card(name: str, owner: str, value, target, fmt_type: str, source: str) -> str:
    formatted_value = fmt_value(value, fmt_type)
    formatted_target = fmt_value(target, fmt_type)

    if value is not None and target and target > 0:
        try:
            pct = float(value) / float(target) * 100
        except (ValueError, TypeError):
            pct = 0
    else:
        pct = 0

    color = pct_class(pct)
    source_badge = '<span class="source-auto">LIVE</span>' if source == "auto" else '<span class="source-sheet">SHEET</span>'

    return f"""
    <div class="kpi-card">
        <div class="kpi-metric-name">{name}</div>
        <div class="kpi-owner">{owner} {source_badge}</div>
        <div class="kpi-value">{formatted_value}</div>
        <div class="kpi-target">Target: {formatted_target}</div>
        <div class="kpi-pct pct-{color}">{pct:.1f}%</div>
        <div class="progress-bg"><div class="progress-fill-{color}" style="width: {min(pct, 100):.0f}%"></div></div>
    </div>
    """


# ============================================================================
# MAIN
# ============================================================================

def main():
    # Header
    st.markdown("""
    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 16px;">
        <div>
            <h1 style="color: #ccd6f6; font-size: 32px; font-weight: 700; margin: 0;">Marketing Scorecard</h1>
            <p style="color: #8892b0; font-size: 14px; margin: 4px 0 0 0;">Q1 2026 KPI Scorecard</p>
        </div>
        <div style="color: #8892b0; font-size: 12px; text-align: right;">
            <span style="color: #64ffda;">●</span> Live &nbsp;
            <span style="color: #ffd666;">●</span> From Sheet
        </div>
    </div>
    """, unsafe_allow_html=True)

    # Load all data
    with st.spinner("Loading data..."):
        b2b_df = load_b2b_weekly_revenue()
        b2c_df = load_b2c_weekly_revenue()
        active_doors = load_active_door_count()
        li_followers = scrape_linkedin_followers()
        ig_followers = scrape_instagram_followers()
        sheet_df = load_google_sheet()

    # ---- Parse Sheet data into a usable structure ----
    # Sheet columns: METRIC, OWNER, TARGET, Current Total, % TO TARGET, 1/1, 1/5, ...
    sheet_weekly = {}  # {metric_key: {date: value}}
    sheet_totals = {}  # {metric_key: current_total}

    if sheet_df is not None and not sheet_df.empty:
        # Normalize column names
        cols = sheet_df.columns.tolist()
        for _, row in sheet_df.iterrows():
            metric_name = str(row.iloc[0]).strip() if len(row) > 0 else ""
            # Map sheet metric name to our key
            key = None
            for mc in METRICS_CONFIG:
                if mc["name"].lower() in metric_name.lower() or metric_name.lower() in mc["name"].lower():
                    key = mc["key"]
                    break
            if not key:
                continue

            # Parse current total
            if len(row) > 3:
                try:
                    total_str = str(row.iloc[3]).replace('$', '').replace(',', '').replace('%', '').strip()
                    if total_str:
                        sheet_totals[key] = float(total_str)
                except (ValueError, TypeError):
                    pass

            # Parse weekly values (columns after the first 5: METRIC, OWNER, TARGET, Current Total, % TO TARGET)
            sheet_weekly[key] = {}
            for col_idx in range(5, len(cols)):
                col_name = str(cols[col_idx]).strip()
                # Parse date from column header like "1/1", "1/5", etc.
                date_match = re.match(r'(\d{1,2})/(\d{1,2})', col_name)
                if date_match:
                    month = int(date_match.group(1))
                    day = int(date_match.group(2))
                    try:
                        col_date = date(2026, month, day)
                        cell_val = str(row.iloc[col_idx]).replace('$', '').replace(',', '').strip()
                        if cell_val and cell_val != '0' and cell_val != '':
                            try:
                                sheet_weekly[key][col_date] = float(cell_val)
                            except ValueError:
                                pass
                    except ValueError:
                        pass

    # ---- Build the combined data grid ----
    # For each metric, build a dict of {week_date: value}

    # B2B weekly revenue — cumulative running total
    b2b_by_week = {}
    if not b2b_df.empty:
        b2b_df['week_start'] = pd.to_datetime(b2b_df['week_start']).dt.date
        cumulative = 0
        for _, row in b2b_df.sort_values('week_start').iterrows():
            cumulative += row['revenue']
            b2b_by_week[row['week_start']] = round(cumulative, 2)

    # B2C weekly revenue — weekly (not cumulative, matching the Excel)
    b2c_by_week = {}
    if not b2c_df.empty:
        b2c_df['week_start'] = pd.to_datetime(b2c_df['week_start']).dt.date
        for _, row in b2c_df.sort_values('week_start').iterrows():
            b2c_by_week[row['week_start']] = round(row['revenue'], 2)

    # Determine current week
    today = date.today()
    current_week = None
    for wd in WEEK_DATES:
        if wd <= today:
            current_week = wd
    if current_week is None:
        current_week = WEEK_DATES[0]

    # Build metric values grid
    metric_data = {}
    for mc in METRICS_CONFIG:
        key = mc["key"]
        weekly = {}

        if key == "retail_revenue":
            # Use BQ data, fall back to sheet for historical
            for wd in WEEK_DATES:
                if wd in b2b_by_week:
                    weekly[wd] = b2b_by_week[wd]
                elif key in sheet_weekly and wd in sheet_weekly[key]:
                    weekly[wd] = sheet_weekly[key][wd]

        elif key == "dtc_revenue":
            for wd in WEEK_DATES:
                if wd in b2c_by_week:
                    weekly[wd] = b2c_by_week[wd]
                elif key in sheet_weekly and wd in sheet_weekly[key]:
                    weekly[wd] = sheet_weekly[key][wd]

        elif key == "active_doors":
            # Current value from BQ, historical from sheet
            if key in sheet_weekly:
                weekly = dict(sheet_weekly[key])
            if active_doors and active_doors > 0:
                weekly[current_week] = active_doors

        elif key == "linkedin_followers":
            if key in sheet_weekly:
                weekly = dict(sheet_weekly[key])
            if li_followers:
                weekly[current_week] = li_followers

        elif key == "instagram_followers":
            if key in sheet_weekly:
                weekly = dict(sheet_weekly[key])
            if ig_followers:
                weekly[current_week] = ig_followers

        else:
            # Manual metrics — all from sheet
            if key in sheet_weekly:
                weekly = dict(sheet_weekly[key])

        metric_data[key] = weekly

    # Compute current totals
    current_values = {}
    for mc in METRICS_CONFIG:
        key = mc["key"]
        weekly = metric_data.get(key, {})

        if key == "retail_revenue":
            # Cumulative — take the latest value
            vals = {k: v for k, v in weekly.items() if k <= today}
            current_values[key] = max(vals.values()) if vals else sheet_totals.get(key)

        elif key == "dtc_revenue":
            # Sum of all weeks
            vals = {k: v for k, v in weekly.items() if k <= today}
            current_values[key] = sum(vals.values()) if vals else sheet_totals.get(key)

        elif key in ("active_doors", "linkedin_followers", "instagram_followers"):
            # Latest snapshot
            vals = {k: v for k, v in weekly.items() if k <= today}
            if vals:
                latest_date = max(vals.keys())
                current_values[key] = vals[latest_date]
            else:
                current_values[key] = sheet_totals.get(key)

        elif key == "rebate_redemption":
            # Cumulative — sum all weekly values
            vals = {k: v for k, v in weekly.items() if k <= today}
            current_values[key] = sum(vals.values()) if vals else sheet_totals.get(key)

        elif key == "rebate_submissions":
            vals = {k: v for k, v in weekly.items() if k <= today}
            current_values[key] = sum(vals.values()) if vals else sheet_totals.get(key)

        elif key == "influencer_posts":
            # Latest cumulative snapshot or sum
            vals = {k: v for k, v in weekly.items() if k <= today}
            if vals:
                latest_date = max(vals.keys())
                current_values[key] = vals[latest_date]
            else:
                current_values[key] = sheet_totals.get(key)

        else:
            # Events — latest snapshot
            vals = {k: v for k, v in weekly.items() if k <= today}
            if vals:
                current_values[key] = sum(vals.values())
            else:
                current_values[key] = sheet_totals.get(key)

    # ---- KPI Cards (2 rows of 5) ----
    for row_start in range(0, 10, 5):
        cols = st.columns(5)
        for i, col in enumerate(cols):
            idx = row_start + i
            if idx >= len(METRICS_CONFIG):
                break
            mc = METRICS_CONFIG[idx]
            val = current_values.get(mc["key"])
            with col:
                st.markdown(
                    render_kpi_card(mc["name"], mc["owner"], val, mc["target"], mc["format"], mc["source"]),
                    unsafe_allow_html=True
                )

    st.markdown("<br>", unsafe_allow_html=True)

    # ---- Weekly Tracker Table ----
    st.markdown('<p class="section-header">Weekly Tracker</p>', unsafe_allow_html=True)

    # Build HTML table
    date_headers = ""
    for wd in WEEK_DATES:
        css_class = "current-week-header" if wd == current_week else ""
        date_headers += f'<th class="{css_class}">{wd.strftime("%-m/%-d")}</th>'

    rows_html = ""
    for mc in METRICS_CONFIG:
        key = mc["key"]
        weekly = metric_data.get(key, {})
        val = current_values.get(key)
        target = mc["target"]
        pct = (float(val) / float(target) * 100) if val and target else 0
        color = pct_class(pct)
        source_badge = '<span class="source-auto">LIVE</span>' if mc["source"] == "auto" else '<span class="source-sheet">SHEET</span>'

        cells = ""
        for wd in WEEK_DATES:
            css_class = "current-week" if wd == current_week else ""
            cell_val = weekly.get(wd)
            formatted = fmt_cell(cell_val, mc["format"])
            cells += f'<td class="{css_class}">{formatted}</td>'

        rows_html += f"""
        <tr>
            <td class="metric-name">{mc["name"]} {source_badge}</td>
            <td class="metric-owner">{mc["owner"]}</td>
            <td>{fmt_value(target, mc["format"])}</td>
            <td><strong>{fmt_value(val, mc["format"])}</strong></td>
            <td class="pct-{color}"><strong>{pct:.1f}%</strong></td>
            {cells}
        </tr>
        """

    # Count rows to estimate height (header + data rows + padding)
    table_row_count = len(METRICS_CONFIG)
    table_height = 44 + (table_row_count * 36) + 20

    table_html = f"""
    <html><head><style>
        body {{ margin: 0; padding: 0; background: transparent; font-family: 'Source Sans Pro', sans-serif; }}
        .tracker-table {{ width: 100%; border-collapse: collapse; font-size: 12px; color: #ccd6f6; }}
        .tracker-table th {{ background: rgba(30,30,47,0.8); padding: 8px 6px; text-align: center; font-weight: 600; color: #8892b0; font-size: 11px; border-bottom: 1px solid rgba(255,255,255,0.1); position: sticky; top: 0; }}
        .tracker-table th.metric-col {{ text-align: left; min-width: 180px; }}
        .tracker-table td {{ padding: 6px; text-align: center; border-bottom: 1px solid rgba(255,255,255,0.05); }}
        .tracker-table td.metric-name {{ text-align: left; font-weight: 600; color: #ccd6f6; }}
        .tracker-table td.metric-owner {{ text-align: left; color: #8892b0; font-size: 11px; }}
        .tracker-table tr:hover {{ background: rgba(255,255,255,0.03); }}
        .current-week {{ background: rgba(0,212,170,0.1) !important; border-left: 2px solid #00d4aa; border-right: 2px solid #00d4aa; }}
        .current-week-header {{ background: rgba(0,212,170,0.2) !important; color: #00d4aa !important; border-left: 2px solid #00d4aa; border-right: 2px solid #00d4aa; }}
        .source-auto {{ color: #64ffda; font-size: 9px; }}
        .source-sheet {{ color: #ffd666; font-size: 9px; }}
        .pct-green {{ color: #64ffda; }}
        .pct-yellow {{ color: #ffd666; }}
        .pct-red {{ color: #ff6b6b; }}
    </style></head><body>
    <div style="overflow-x: auto; border-radius: 8px; border: 1px solid rgba(255,255,255,0.1);">
    <table class="tracker-table">
        <thead>
            <tr>
                <th class="metric-col">Metric</th>
                <th>Owner</th>
                <th>Target</th>
                <th>Current</th>
                <th>% to Target</th>
                {date_headers}
            </tr>
        </thead>
        <tbody>
            {rows_html}
        </tbody>
    </table>
    </div>
    </body></html>
    """
    st.html(table_html)

    st.markdown("<br>", unsafe_allow_html=True)

    # ---- Trend Sparklines ----
    st.markdown('<p class="section-header">Trends</p>', unsafe_allow_html=True)

    # Show sparklines for key metrics in a 2x3 grid
    sparkline_metrics = [mc for mc in METRICS_CONFIG if metric_data.get(mc["key"])]
    cols_per_row = 3
    for row_start in range(0, len(sparkline_metrics), cols_per_row):
        cols = st.columns(cols_per_row)
        for i, col in enumerate(cols):
            idx = row_start + i
            if idx >= len(sparkline_metrics):
                break
            mc = sparkline_metrics[idx]
            weekly = metric_data.get(mc["key"], {})

            # Build sorted arrays
            dates = sorted(weekly.keys())
            values = [weekly[d] for d in dates]

            if not values:
                continue

            with col:
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=[d.strftime("%-m/%-d") for d in dates],
                    y=values,
                    mode='lines+markers',
                    line=dict(color='#00d4aa', width=2),
                    marker=dict(size=5),
                    fill='tozeroy',
                    fillcolor='rgba(0, 212, 170, 0.1)',
                    hovertemplate='%{x}: %{y:,.0f}<extra></extra>',
                ))

                # Target line
                fig.add_hline(
                    y=mc["target"],
                    line_dash="dot",
                    line_color="rgba(255,214,102,0.5)",
                    annotation_text="Target",
                    annotation_font_color="#ffd666",
                    annotation_font_size=10,
                )

                fig.update_layout(
                    title=dict(text=mc["name"], font=dict(size=13, color='#ccd6f6'), x=0),
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(0,0,0,0)',
                    font=dict(color='#8892b0', size=10),
                    height=200,
                    margin=dict(l=0, r=0, t=30, b=0),
                    xaxis=dict(
                        gridcolor='rgba(255,255,255,0.05)',
                        linecolor='rgba(255,255,255,0.1)',
                        showgrid=False,
                    ),
                    yaxis=dict(
                        gridcolor='rgba(255,255,255,0.05)',
                        linecolor='rgba(255,255,255,0.1)',
                    ),
                    showlegend=False,
                )
                st.plotly_chart(fig, width="stretch")

    # Footer
    st.markdown(f"""
    <div style="text-align: center; color: #8892b0; margin-top: 32px; padding: 16px; border-top: 1px solid rgba(255,255,255,0.1);">
        <p style="margin: 0; font-size: 12px;">Last refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M')} &nbsp;|&nbsp;
        BQ data refreshes every 5 min &nbsp;|&nbsp; Social data refreshes hourly &nbsp;|&nbsp; Sheet data refreshes every 5 min</p>
    </div>
    """, unsafe_allow_html=True)


if __name__ == "__main__":
    main()
