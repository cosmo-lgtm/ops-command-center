"""
Hemp Industry Economic Impact Dashboard
Data-driven advocacy for policymakers and stakeholders
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from google.cloud import bigquery

# Dark mode CSS with emerald green accent
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600;700&display=swap');

    .stApp {
        background: linear-gradient(135deg, #0a0a0a 0%, #1a1a2e 50%, #0f0f1a 100%);
    }
    html, body, [class*="css"] {
        font-family: 'Space Grotesk', sans-serif;
        color: #e0e0e0;
    }
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}

    .metric-card {
        background: rgba(255, 255, 255, 0.03);
        backdrop-filter: blur(20px);
        border: 1px solid rgba(255, 255, 255, 0.08);
        border-radius: 20px;
        padding: 28px;
        text-align: center;
        transition: all 0.3s ease;
        position: relative;
        overflow: hidden;
    }
    .metric-card::before {
        content: '';
        position: absolute;
        top: 0; left: 0; right: 0;
        height: 2px;
        background: linear-gradient(90deg, #10b981, #34d399, #6ee7b7);
    }
    .metric-card:hover {
        transform: translateY(-4px);
        border-color: rgba(16, 185, 129, 0.3);
        box-shadow: 0 20px 40px rgba(16, 185, 129, 0.1);
    }
    .metric-value {
        font-size: 2.8rem;
        font-weight: 700;
        background: linear-gradient(135deg, #10b981 0%, #34d399 50%, #6ee7b7 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 8px;
    }
    .metric-label {
        font-size: 0.85rem;
        color: #9ca3af;
        text-transform: uppercase;
        letter-spacing: 1.5px;
        font-weight: 500;
    }
    .metric-change {
        font-size: 0.8rem;
        color: #10b981;
        margin-top: 12px;
        display: inline-flex;
        align-items: center;
        gap: 4px;
        background: rgba(16, 185, 129, 0.1);
        padding: 4px 12px;
        border-radius: 20px;
    }
    .section-header {
        font-size: 1.4rem;
        font-weight: 600;
        color: #f3f4f6;
        padding-bottom: 12px;
        margin: 48px 0 24px 0;
        position: relative;
        display: inline-block;
    }
    .section-header::after {
        content: '';
        position: absolute;
        bottom: 0; left: 0;
        width: 60px; height: 3px;
        background: linear-gradient(90deg, #10b981, transparent);
        border-radius: 2px;
    }
    .source-citation {
        font-size: 0.7rem;
        color: #6b7280;
        margin-top: 12px;
    }
    .stat-card {
        background: rgba(255, 255, 255, 0.02);
        backdrop-filter: blur(10px);
        border: 1px solid rgba(255, 255, 255, 0.05);
        border-radius: 16px;
        padding: 32px;
        text-align: center;
    }
    .stat-value {
        font-size: 3.5rem;
        font-weight: 700;
        background: linear-gradient(135deg, #10b981, #34d399);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }
    .stat-label {
        color: #9ca3af;
        font-size: 0.9rem;
        margin-top: 8px;
        line-height: 1.5;
    }
    .timeline-item {
        display: flex;
        margin: 16px 0;
        padding: 20px;
        background: rgba(255, 255, 255, 0.02);
        backdrop-filter: blur(10px);
        border-radius: 12px;
        border: 1px solid rgba(255, 255, 255, 0.05);
        transition: all 0.2s ease;
    }
    .timeline-item:hover {
        background: rgba(255, 255, 255, 0.04);
        border-color: rgba(16, 185, 129, 0.2);
    }
    .timeline-date {
        min-width: 100px;
        font-weight: 600;
        color: #6b7280;
        font-size: 0.85rem;
    }
    .timeline-title {
        font-weight: 600;
        color: #f3f4f6;
        margin-bottom: 4px;
    }
    .timeline-desc {
        color: #9ca3af;
        font-size: 0.85rem;
    }
    .key-section h3 {
        color: #10b981;
        font-size: 1.1rem;
        margin-bottom: 16px;
    }
    .key-section ul { list-style: none; padding: 0; }
    .key-section li {
        padding: 8px 0;
        color: #d1d5db;
        border-bottom: 1px solid rgba(255,255,255,0.05);
    }
    .key-section li:last-child { border-bottom: none; }
    .status-item {
        display: flex;
        align-items: center;
        margin: 10px 0;
        padding: 8px 12px;
        background: rgba(255,255,255,0.02);
        border-radius: 8px;
    }
    .status-dot {
        width: 12px; height: 12px;
        border-radius: 50%;
        margin-right: 12px;
    }
    .status-text { color: #d1d5db; font-size: 0.9rem; }
    .status-count { font-weight: 700; color: #f3f4f6; }
    .obs-card {
        background: rgba(16, 185, 129, 0.05);
        border: 1px solid rgba(16, 185, 129, 0.1);
        border-radius: 12px;
        padding: 20px;
    }
    .obs-card h3 { color: #10b981; font-size: 1rem; margin-bottom: 12px; }
    .obs-card ul { margin: 0; padding-left: 20px; }
    .obs-card li { color: #d1d5db; padding: 4px 0; font-size: 0.9rem; }
</style>
""", unsafe_allow_html=True)


@st.cache_resource
def get_bq_client():
    if "gcp_service_account" in st.secrets:
        from google.oauth2 import service_account
        credentials = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])
        return bigquery.Client(project='artful-logic-475116-p1', credentials=credentials)
    return bigquery.Client(project='artful-logic-475116-p1')


@st.cache_data(ttl=3600)
def load_data(query):
    client = get_bq_client()
    return client.query(query).to_dataframe()


@st.cache_data(ttl=3600)
def load_all_data():
    data = {}
    data['production'] = load_data("SELECT * FROM `artful-logic-475116-p1.hemp_advocacy.production_by_state` ORDER BY year, hemp_type")
    data['market'] = load_data("SELECT * FROM `artful-logic-475116-p1.hemp_advocacy.market_metrics` ORDER BY year")
    data['employment'] = load_data("SELECT * FROM `artful-logic-475116-p1.hemp_advocacy.employment_stats` ORDER BY year")
    data['regulatory'] = load_data("SELECT * FROM `artful-logic-475116-p1.hemp_advocacy.regulatory_status` ORDER BY state")
    data['tax'] = load_data("SELECT * FROM `artful-logic-475116-p1.hemp_advocacy.tax_revenue` ORDER BY year, state")
    data['timeline'] = load_data("SELECT * FROM `artful-logic-475116-p1.hemp_advocacy.industry_timeline` ORDER BY event_date")
    return data


data = load_all_data()

# HEADER
st.markdown("""
<div style="text-align: center; padding: 40px 0 50px 0;">
    <p style="color: #10b981; font-size: 0.85rem; letter-spacing: 3px; text-transform: uppercase; margin-bottom: 16px;">Economic Impact Report</p>
    <h1 style="color: #f3f4f6; font-size: 3rem; font-weight: 700; margin-bottom: 16px; line-height: 1.2;">
        Hemp Industry<br/><span style="background: linear-gradient(135deg, #10b981, #34d399); -webkit-background-clip: text; -webkit-text-fill-color: transparent;">by the Numbers</span>
    </h1>
    <p style="color: #6b7280; font-size: 1rem; max-width: 600px; margin: 0 auto;">
        Data-driven insights on jobs, growth, and consumer choice in the hemp-derived products sector
    </p>
</div>
""", unsafe_allow_html=True)

# HERO METRICS
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.markdown('<div class="metric-card"><div class="metric-value">$445M</div><div class="metric-label">Production Value</div><div class="metric-change">↑ 40% YoY</div></div>', unsafe_allow_html=True)
with col2:
    st.markdown('<div class="metric-card"><div class="metric-value">440K+</div><div class="metric-label">Jobs Created</div><div class="metric-change">↑ 5.4% YoY</div></div>', unsafe_allow_html=True)
with col3:
    st.markdown('<div class="metric-card"><div class="metric-value">$4.4B</div><div class="metric-label">Tax Revenue</div><div class="metric-change">2024 Total</div></div>', unsafe_allow_html=True)
with col4:
    st.markdown('<div class="metric-card"><div class="metric-value">45.3K</div><div class="metric-label">Acres Planted</div><div class="metric-change">↑ 64% YoY</div></div>', unsafe_allow_html=True)

st.markdown("<p class='source-citation' style='text-align:center; margin-top: 24px;'>Sources: USDA NASS 2025, Vangst Jobs Report, MPP Tax Revenue Analysis</p>", unsafe_allow_html=True)

# MARKET VALUE FLOW - Sankey
st.markdown("<div class='section-header'>Market Value Flow</div>", unsafe_allow_html=True)

fig_sankey = go.Figure(data=[go.Sankey(
    node=dict(
        pad=20, thickness=25,
        line=dict(color='rgba(255,255,255,0.1)', width=1),
        label=['Hemp Production<br>$445M', 'Processing &<br>Manufacturing', 'CBD Products<br>$180M',
               'THC Beverages<br>$120M', 'Fiber/Grain<br>$145M', 'Wholesale &<br>Distribution', 'Retail Sales<br>$1.8B'],
        color=['#047857', '#059669', '#10b981', '#34d399', '#6ee7b7', '#10b981', '#34d399'],
        x=[0.0, 0.25, 0.5, 0.5, 0.5, 0.75, 1.0],
        y=[0.5, 0.5, 0.15, 0.5, 0.85, 0.5, 0.5]
    ),
    link=dict(
        source=[0, 1, 1, 1, 2, 3, 4, 5],
        target=[1, 2, 3, 4, 5, 5, 5, 6],
        value=[445, 180, 120, 145, 540, 360, 435, 1800],
        color=['rgba(16,185,129,0.3)', 'rgba(52,211,153,0.3)', 'rgba(110,231,183,0.3)',
               'rgba(167,243,208,0.3)', 'rgba(16,185,129,0.25)', 'rgba(52,211,153,0.25)',
               'rgba(110,231,183,0.25)', 'rgba(16,185,129,0.3)'],
        hovertemplate='%{source.label} → %{target.label}<br>$%{value}M<extra></extra>'
    )
)])
fig_sankey.update_layout(
    title=dict(text="Hemp Industry Value Chain ($M)", font=dict(size=18, color='#f3f4f6')),
    height=450, paper_bgcolor='rgba(0,0,0,0)',
    font=dict(color='#d1d5db', size=12, family='Space Grotesk'),
    margin=dict(t=60, l=20, r=20, b=20)
)
st.plotly_chart(fig_sankey, use_container_width=True)

col1, col2 = st.columns(2)

with col1:
    # Donut chart
    fig_donut = go.Figure(data=[go.Pie(
        labels=['CBD Products', 'THC Beverages', 'Fiber Products', 'Seed & Oil'],
        values=[180, 120, 100, 45],
        hole=0.6,
        marker=dict(colors=['#10b981', '#34d399', '#6ee7b7', '#a7f3d0'], line=dict(color='#1a1a2e', width=2)),
        textinfo='label+percent', textposition='outside',
        textfont=dict(color='#d1d5db', size=12),
        hovertemplate='<b>%{label}</b><br>$%{value}M<br>%{percent}<extra></extra>'
    )])
    fig_donut.add_annotation(text='<b>$445M</b><br>Total', x=0.5, y=0.5,
                             font=dict(size=20, color='#10b981', family='Space Grotesk'), showarrow=False)
    fig_donut.update_layout(
        title=dict(text="Market Breakdown by Segment", font=dict(size=16, color='#f3f4f6')),
        height=420, paper_bgcolor='rgba(0,0,0,0)', showlegend=False, margin=dict(t=60, l=20, r=20, b=20)
    )
    st.plotly_chart(fig_donut, use_container_width=True)

with col2:
    # Area chart
    years = [2023, 2024, 2025, 2026, 2027, 2028, 2029, 2030]
    values = [1.8, 2.2, 2.7, 3.3, 4.0, 4.9, 6.0, 7.8]
    fig_area = go.Figure()
    fig_area.add_trace(go.Scatter(
        x=years, y=values, mode='lines+markers+text', fill='tozeroy',
        fillcolor='rgba(16, 185, 129, 0.15)', line=dict(color='#10b981', width=3),
        marker=dict(size=10, color='#10b981', line=dict(width=2, color='#0a0a0a')),
        text=[f'${v}B' for v in values], textposition='top center', textfont=dict(color='#10b981', size=11),
        hovertemplate='<b>%{x}</b><br>$%{y}B<extra></extra>'
    ))
    fig_area.update_layout(
        title=dict(text="Market Growth Trajectory (21.1% CAGR)", font=dict(size=16, color='#f3f4f6')),
        height=420, paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
        font=dict(family='Space Grotesk', color='#9ca3af'),
        xaxis=dict(gridcolor='rgba(255,255,255,0.05)', tickfont=dict(color='#9ca3af'), dtick=1),
        yaxis=dict(gridcolor='rgba(255,255,255,0.05)', tickfont=dict(color='#9ca3af'), tickprefix='$', ticksuffix='B', range=[0, 9]),
        margin=dict(t=60, l=60, r=20, b=40), showlegend=False
    )
    st.plotly_chart(fig_area, use_container_width=True)

st.markdown("<p class='source-citation'>Sources: USDA NASS, Grand View Research (21.1% CAGR), Industry Analysis</p>", unsafe_allow_html=True)

# REGULATORY MAP
st.markdown("<div class='section-header'>Regulatory Landscape</div>", unsafe_allow_html=True)
status_counts = data['regulatory']['thc_beverage_status'].value_counts()
col1, col2, col3 = st.columns([1, 2.5, 1])

with col1:
    st.markdown("#### By Status")
    status_colors = {'legal': '#10b981', 'legal_restricted': '#34d399', 'pending': '#fbbf24', 'dispensary_only': '#f97316', 'banned': '#ef4444'}
    for status, count in status_counts.items():
        color = status_colors.get(status, '#6b7280')
        label = status.replace('_', ' ').title()
        st.markdown(f'<div class="status-item"><div class="status-dot" style="background: {color};"></div><span class="status-text"><span class="status-count">{count}</span> — {label}</span></div>', unsafe_allow_html=True)

with col2:
    reg_df = data['regulatory'].copy()
    status_map = {'legal': 4, 'legal_restricted': 3, 'pending': 2, 'dispensary_only': 1, 'banned': 0}
    reg_df['status_num'] = reg_df['thc_beverage_status'].map(status_map)
    fig = go.Figure(data=go.Choropleth(
        locations=reg_df['state'], z=reg_df['status_num'], locationmode='USA-states',
        colorscale=[[0, '#ef4444'], [0.25, '#f97316'], [0.5, '#fbbf24'], [0.75, '#34d399'], [1, '#10b981']],
        showscale=False, hovertemplate="<b>%{location}</b><br>%{text}<extra></extra>",
        text=reg_df['thc_beverage_status'].str.replace('_', ' ').str.title(),
        marker_line_color='#1a1a2e', marker_line_width=1
    ))
    fig.update_layout(
        geo=dict(scope='usa', projection=dict(type='albers usa'), showlakes=False, bgcolor='rgba(0,0,0,0)', landcolor='rgba(255,255,255,0.02)'),
        margin=dict(l=0, r=0, t=0, b=0), height=420, paper_bgcolor='rgba(0,0,0,0)'
    )
    st.plotly_chart(fig, use_container_width=True)

with col3:
    st.markdown('<div class="obs-card"><h3>Key Stats</h3><ul><li><strong>28</strong> states legal</li><li><strong>21+</strong> age required</li><li><strong>5-10mg</strong> THC limits</li><li><strong>27+</strong> active bills</li></ul></div>', unsafe_allow_html=True)

st.markdown("<p class='source-citation'>Source: MultiState Insider, Vicente LLP (Nov 2025)</p>", unsafe_allow_html=True)

# JOBS & TAX
st.markdown("<div class='section-header'>Economic Contribution</div>", unsafe_allow_html=True)
col1, col2 = st.columns(2)

with col1:
    fig_funnel = go.Figure(go.Funnel(
        y=['Total Industry', 'Direct Employment', 'Cannabis Sector', 'Hemp-Specific', 'New Hires 2024'],
        x=[440000, 320000, 280000, 160000, 23760],
        textposition='inside', textinfo='value+percent initial', opacity=0.85,
        marker=dict(color=['#059669', '#10b981', '#34d399', '#6ee7b7', '#a7f3d0'], line=dict(width=2, color='#1a1a2e')),
        connector=dict(line=dict(color='rgba(255,255,255,0.1)', width=2))
    ))
    fig_funnel.update_layout(
        title=dict(text="Employment Funnel (440K+ Jobs)", font=dict(size=16, color='#f3f4f6')),
        height=400, paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
        font=dict(color='#d1d5db', family='Space Grotesk'), margin=dict(t=60, l=10, r=10, b=10)
    )
    st.plotly_chart(fig_funnel, use_container_width=True)

with col2:
    tax_states = data['tax'][(data['tax']['state'] != 'US') & (data['tax']['year'] == 2023) & (data['tax']['quarter'] == 4)].nlargest(10, 'tax_revenue_usd')
    fig_tree = go.Figure(go.Treemap(
        labels=tax_states['state'].tolist() + ['Other States'],
        parents=[''] * len(tax_states) + [''],
        values=tax_states['tax_revenue_usd'].tolist() + [500_000_000],
        textinfo='label+value', texttemplate='<b>%{label}</b><br>$%{value:,.0f}',
        marker=dict(colors=['#047857', '#059669', '#10b981', '#34d399', '#6ee7b7', '#a7f3d0', '#d1fae5', '#ecfdf5', '#f0fdf4', '#fafafa', '#4b5563'],
                    line=dict(width=2, color='#1a1a2e')),
        textfont=dict(size=12), hovertemplate='<b>%{label}</b><br>Tax Revenue: $%{value:,.0f}<extra></extra>'
    ))
    fig_tree.update_layout(
        title=dict(text="State Tax Revenue Treemap", font=dict(size=16, color='#f3f4f6')),
        height=400, paper_bgcolor='rgba(0,0,0,0)', margin=dict(t=60, l=10, r=10, b=10), font=dict(family='Space Grotesk')
    )
    st.plotly_chart(fig_tree, use_container_width=True)

st.markdown("<p class='source-citation'>Sources: Vangst 2024, U.S. Census Bureau, MPP Analysis</p>", unsafe_allow_html=True)

# CONSUMER DEMAND
st.markdown("<div class='section-header'>Consumer Demand</div>", unsafe_allow_html=True)
col1, col2 = st.columns([1.2, 1])

with col1:
    categories = ['Awareness', 'Purchase Intent', 'Price Premium', 'Brand Loyalty', 'Repeat Purchase', 'Social Share']
    fig_radar = go.Figure()
    fig_radar.add_trace(go.Scatterpolar(r=[85, 72, 70, 65, 78, 82], theta=categories, fill='toself',
                                         fillcolor='rgba(16, 185, 129, 0.2)', line=dict(color='#10b981', width=2), name='Gen Z (18-25)'))
    fig_radar.add_trace(go.Scatterpolar(r=[78, 68, 65, 70, 72, 68], theta=categories, fill='toself',
                                         fillcolor='rgba(52, 211, 153, 0.15)', line=dict(color='#34d399', width=2), name='Millennials (26-41)'))
    fig_radar.add_trace(go.Scatterpolar(r=[64, 45, 48, 55, 58, 35], theta=categories, fill='toself',
                                         fillcolor='rgba(110, 231, 183, 0.1)', line=dict(color='#6ee7b7', width=2), name='Gen X (42-57)'))
    fig_radar.update_layout(
        title=dict(text="Consumer Segment Analysis", font=dict(size=16, color='#f3f4f6')),
        polar=dict(
            radialaxis=dict(visible=True, range=[0, 100], gridcolor='rgba(255,255,255,0.1)', tickfont=dict(color='#6b7280', size=10)),
            angularaxis=dict(gridcolor='rgba(255,255,255,0.1)', tickfont=dict(color='#9ca3af', size=11)),
            bgcolor='rgba(0,0,0,0)'
        ),
        showlegend=True, legend=dict(orientation='h', yanchor='bottom', y=-0.15, xanchor='center', x=0.5, font=dict(color='#9ca3af', size=11)),
        height=420, paper_bgcolor='rgba(0,0,0,0)', font=dict(family='Space Grotesk'), margin=dict(t=60, l=60, r=60, b=80)
    )
    st.plotly_chart(fig_radar, use_container_width=True)

with col2:
    st.markdown("""
    <div style="padding: 20px;">
        <div class="stat-card" style="margin-bottom: 20px;">
            <div class="stat-value" style="font-size: 2.8rem;">64%</div>
            <div class="stat-label">of Americans familiar<br/>with CBD products</div>
        </div>
        <div class="stat-card" style="margin-bottom: 20px;">
            <div class="stat-value" style="font-size: 2.8rem;">36.5%</div>
            <div class="stat-label">CBD beverage sales<br/>growth in 2023</div>
        </div>
        <div class="stat-card">
            <div class="stat-value" style="font-size: 2.8rem;">70%</div>
            <div class="stat-label">Gen Z/Millennials pay<br/>premium for traceable</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

st.markdown("<p class='source-citation' style='margin-top: 24px;'>Sources: Mastermind Behavior, Euromonitor, Industry Surveys 2024</p>", unsafe_allow_html=True)

# TIMELINE
st.markdown("<div class='section-header'>Industry Timeline</div>", unsafe_allow_html=True)
timeline_df = data['timeline'].sort_values('event_date')
for _, row in timeline_df.iterrows():
    impact_color = {'positive': '#10b981', 'negative': '#ef4444', 'neutral': '#6b7280'}.get(row['impact'], '#6b7280')
    st.markdown(f'''
    <div class="timeline-item" style="border-left: 3px solid {impact_color};">
        <div class="timeline-date">{row["event_date"].strftime("%b %Y")}</div>
        <div>
            <div class="timeline-title">{row["title"]}</div>
            <div class="timeline-desc">{row["description"]}</div>
        </div>
    </div>
    ''', unsafe_allow_html=True)

# KEY TAKEAWAYS
st.markdown("<div class='section-header'>Key Takeaways</div>", unsafe_allow_html=True)
col1, col2 = st.columns(2)
with col1:
    st.markdown("""
<div class="key-section">

### Economic Impact
- **$445M** U.S. hemp production value (2024)
- **$4.4B** state tax revenue generated
- **440K+** jobs across the industry
- **40%+** annual production growth

### Agricultural Benefits
- **45,294** acres under cultivation
- **8,153** farming operations
- Rural economic diversification
- Federal crop insurance eligible

</div>
""", unsafe_allow_html=True)

with col2:
    st.markdown("""
<div class="key-section">

### Consumer Choice
- **28 states** with legal THC beverages
- Strong demand & awareness
- Preference for regulated products
- Universal 21+ age restriction

### Regulatory Landscape
- Responsible state frameworks emerging
- 5-10mg serving limits standard
- Industry supports sensible regulation
- Clear labeling requirements in place

</div>
""", unsafe_allow_html=True)

# FOOTER
st.markdown("---")
st.markdown('''
<div style="text-align: center; color: #4b5563; padding: 30px 0;">
    <p style="font-size: 0.75rem; margin-bottom: 8px;">
        <strong style="color: #6b7280;">Data Sources:</strong> USDA NASS • Census Bureau • Vangst • Grand View Research • MultiState • Vicente LLP
    </p>
    <p style="font-size: 0.7rem; color: #4b5563;">
        Last updated November 2025 • Data refreshed quarterly
    </p>
</div>
''', unsafe_allow_html=True)
