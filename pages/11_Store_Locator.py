"""
Store Locator POC
Consumer-facing store finder showing retailers with products ordered in last 90 days.
Data source: staging_vip.store_locator_cache (refreshed daily)
"""

import streamlit as st
import pandas as pd
from google.cloud import bigquery
import pydeck as pdk

# Page config
st.set_page_config(
    page_title="Store Locator",
    page_icon="📍",
    layout="wide"
)

# Minimal CSS for clean consumer look
st.markdown("""
<style>
    .stApp {
        background: #ffffff;
    }

    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}

    .store-card {
        background: #f8f9fa;
        border-radius: 12px;
        padding: 16px;
        margin-bottom: 12px;
        border: 1px solid #e9ecef;
    }

    .store-name {
        font-size: 1.1rem;
        font-weight: 600;
        color: #212529;
        margin-bottom: 4px;
    }

    .store-address {
        font-size: 0.9rem;
        color: #6c757d;
        margin-bottom: 8px;
    }

    .product-tag {
        display: inline-block;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 4px 10px;
        border-radius: 16px;
        font-size: 0.75rem;
        margin-right: 6px;
        margin-bottom: 6px;
    }

    .rating {
        color: #ffc107;
        font-size: 0.85rem;
    }

    .search-container {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 32px;
        border-radius: 16px;
        margin-bottom: 24px;
    }

    .search-title {
        color: white;
        font-size: 1.5rem;
        font-weight: 600;
        margin-bottom: 8px;
    }

    .search-subtitle {
        color: rgba(255,255,255,0.8);
        font-size: 0.95rem;
        margin-bottom: 16px;
    }
</style>
""", unsafe_allow_html=True)


@st.cache_resource
def get_bq_client():
    return bigquery.Client()


@st.cache_data(ttl=3600)
def get_available_states():
    """Get list of states with stores for dropdown."""
    client = get_bq_client()
    query = """
    SELECT DISTINCT state
    FROM `staging_vip.store_locator_cache`
    WHERE state IS NOT NULL
    ORDER BY state
    """
    df = client.query(query).to_dataframe()
    return df['state'].tolist()


@st.cache_data(ttl=3600)
def load_stores(state_filter: str):
    """Load stores from pre-computed cache table."""
    client = get_bq_client()

    query = f"""
    SELECT *
    FROM `staging_vip.store_locator_cache`
    WHERE state = '{state_filter}'
    """

    df = client.query(query).to_dataframe()
    return df


def render_store_card(store):
    """Render a single store card."""
    # Build product tags from boolean flags
    products_html = ""
    if store.get('has_bottles'):
        products_html += '<span class="product-tag">Bottles</span>'
    if store.get('has_cans'):
        products_html += '<span class="product-tag">Cans</span>'
    if store.get('has_shots'):
        products_html += '<span class="product-tag">Shots</span>'

    rating_html = ""
    if pd.notna(store.get('rating')):
        stars = "★" * int(store['rating']) + "☆" * (5 - int(store['rating']))
        rating_html = f'<span class="rating">{stars} {store["rating"]:.1f} ({int(store["reviews"])} reviews)</span>'

    st.markdown(f"""
    <div class="store-card">
        <div class="store-name">{store['store_name']}</div>
        <div class="store-address">{store['street_address']}, {store['city']}, {store['state']} {store['zip']}</div>
        {rating_html}
        <div style="margin-top: 8px;">{products_html}</div>
    </div>
    """, unsafe_allow_html=True)


def main():
    # Search header
    st.markdown("""
    <div class="search-container">
        <div class="search-title">Find Nowadays Near You</div>
        <div class="search-subtitle">Discover retailers carrying your favorite THC beverages</div>
    </div>
    """, unsafe_allow_html=True)

    # Get states list first (fast query from cache)
    states_list = get_available_states()

    # Filters row
    col1, col2, col3, col4, col5 = st.columns([2, 1, 1, 1, 2])

    with col1:
        selected_state = st.selectbox("Select State", ["-- Select a State --"] + states_list)

    # Only proceed after state is selected
    if selected_state == "-- Select a State --":
        st.info("Select a state to find stores near you")
        return

    # Load data for selected state
    with st.spinner(f"Loading stores in {selected_state}..."):
        df = load_stores(state_filter=selected_state)

    if len(df) == 0:
        st.warning(f"No stores found in {selected_state}")
        return

    # Product category checkboxes
    with col2:
        show_bottles = st.checkbox("Bottles", value=True)
    with col3:
        show_cans = st.checkbox("Cans", value=True)
    with col4:
        show_shots = st.checkbox("Shots", value=True)

    with col5:
        search = st.text_input("Search city or store", placeholder="e.g., Austin, Total Wine")

    # Filter data
    filtered = df.copy()

    # Apply product filters (OR logic - show if has ANY selected category)
    if not (show_bottles and show_cans and show_shots):
        mask = pd.Series([False] * len(filtered))
        if show_bottles:
            mask = mask | filtered['has_bottles'].fillna(False)
        if show_cans:
            mask = mask | filtered['has_cans'].fillna(False)
        if show_shots:
            mask = mask | filtered['has_shots'].fillna(False)
        filtered = filtered[mask]

    if search:
        search_lower = search.lower()
        filtered = filtered[
            filtered['city'].str.lower().str.contains(search_lower, na=False) |
            filtered['store_name'].str.lower().str.contains(search_lower, na=False)
        ]

    # Stats
    st.markdown(f"**{len(filtered):,}** stores found")

    # Map and list layout
    map_col, list_col = st.columns([2, 1])

    with map_col:
        if len(filtered) > 0:
            # Calculate center
            center_lat = filtered['lat'].mean()
            center_lon = filtered['lon'].mean()

            # Create map layer
            layer = pdk.Layer(
                "ScatterplotLayer",
                data=filtered,
                get_position=["lon", "lat"],
                get_fill_color=[102, 126, 234, 200],
                get_radius=5000,
                pickable=True,
                auto_highlight=True,
            )

            # Tooltip
            tooltip = {
                "html": "<b>{store_name}</b><br/>{city}, {state}",
                "style": {"backgroundColor": "#1a1a2e", "color": "white"}
            }

            # View state
            view = pdk.ViewState(
                latitude=center_lat,
                longitude=center_lon,
                zoom=6,
                pitch=0
            )

            # Render map
            st.pydeck_chart(pdk.Deck(
                layers=[layer],
                initial_view_state=view,
                tooltip=tooltip,
                map_style="mapbox://styles/mapbox/light-v10"
            ))
        else:
            st.info("No stores match your search criteria")

    with list_col:
        st.markdown("### Nearby Stores")

        # Show first 20 stores
        for _, store in filtered.head(20).iterrows():
            render_store_card(store)

        if len(filtered) > 20:
            st.caption(f"Showing 20 of {len(filtered)} stores. Refine your search to see more.")


if __name__ == "__main__":
    main()
