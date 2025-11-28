"""
Ops Command Center
Multi-page Streamlit dashboard consolidating all operations analytics.
"""

import streamlit as st

# Page config - MUST be first Streamlit command
st.set_page_config(
    page_title="Ops Command Center",
    page_icon="🎛️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Dark mode custom CSS
st.markdown("""
<style>
    /* Main background */
    .stApp {
        background: linear-gradient(135deg, #0f0f1a 0%, #1a1a2e 50%, #16213e 100%);
    }

    /* Sidebar styling */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #1a1a2e 0%, #16213e 100%);
    }

    [data-testid="stSidebar"] .stMarkdown {
        color: #ccd6f6;
    }

    /* Hide Streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}

    /* Dashboard card */
    .dashboard-card {
        background: linear-gradient(145deg, #1e1e2f 0%, #2a2a4a 100%);
        border-radius: 16px;
        padding: 32px;
        border: 1px solid rgba(255,255,255,0.1);
        box-shadow: 0 8px 32px rgba(0,0,0,0.3);
        margin-bottom: 24px;
        transition: transform 0.3s ease, box-shadow 0.3s ease;
    }

    .dashboard-card:hover {
        transform: translateY(-4px);
        box-shadow: 0 12px 40px rgba(0,0,0,0.4);
    }

    .card-icon {
        font-size: 48px;
        margin-bottom: 16px;
    }

    .card-title {
        font-size: 24px;
        font-weight: 700;
        color: #ccd6f6;
        margin-bottom: 8px;
    }

    .card-description {
        font-size: 14px;
        color: #8892b0;
        line-height: 1.6;
    }

    /* Header styling */
    .main-header {
        background: linear-gradient(90deg, #667eea 0%, #764ba2 50%, #f093fb 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-size: 56px;
        font-weight: 800;
        margin-bottom: 8px;
        text-align: center;
    }

    .main-subtitle {
        color: #8892b0;
        font-size: 18px;
        margin-bottom: 48px;
        text-align: center;
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
</style>
""", unsafe_allow_html=True)


def main():
    # Header
    st.markdown("""
    <div style="text-align: center; margin-bottom: 48px;">
        <h1 class="main-header">Ops Command Center</h1>
        <p class="main-subtitle">Unified Operations Analytics Dashboard</p>
        <div class="live-indicator" style="justify-content: center;">
            <span class="live-dot"></span>
            All Systems Operational
        </div>
    </div>
    """, unsafe_allow_html=True)

    # Dashboard cards in a 2x2 grid
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("""
        <div class="dashboard-card">
            <div class="card-icon">🔬</div>
            <div class="card-title">Data Quality</div>
            <div class="card-description">
                VIP ↔ Salesforce alignment, match rates, duplicate detection, and data completeness metrics.
                <br><br>
                <strong>Key Metrics:</strong> Health Score, Retail Match Rate, Distributor Alignment
            </div>
        </div>
        """, unsafe_allow_html=True)

        st.markdown("""
        <div class="dashboard-card">
            <div class="card-icon">📦</div>
            <div class="card-title">ShipStation Fulfillment</div>
            <div class="card-description">
                B2B order fulfillment analytics - shipping costs, carrier performance, and delivery metrics.
                <br><br>
                <strong>Key Metrics:</strong> Fulfillment Rate, Days to Ship, Shipping Cost MTD
            </div>
        </div>
        """, unsafe_allow_html=True)

    with col2:
        st.markdown("""
        <div class="dashboard-card">
            <div class="card-icon">📊</div>
            <div class="card-title">Distributor Inventory</div>
            <div class="card-description">
                Salesforce orders vs VIP depletion - weeks of inventory, overstock/understock by distributor.
                <br><br>
                <strong>Key Metrics:</strong> O/D Ratio, Weeks of Inventory, Product Velocity
            </div>
        </div>
        """, unsafe_allow_html=True)

        st.markdown("""
        <div class="dashboard-card">
            <div class="card-icon">🎯</div>
            <div class="card-title">Zendesk Support</div>
            <div class="card-description">
                B2C customer support analytics - CSAT scores, resolution times, agent performance.
                <br><br>
                <strong>Key Metrics:</strong> CSAT Rate, Avg Resolution, Open Backlog
            </div>
        </div>
        """, unsafe_allow_html=True)

    # Navigation hint
    st.markdown("""
    <div style="text-align: center; margin-top: 48px; color: #8892b0;">
        <p>👈 Select a dashboard from the sidebar to view detailed analytics</p>
    </div>
    """, unsafe_allow_html=True)


if __name__ == "__main__":
    main()
