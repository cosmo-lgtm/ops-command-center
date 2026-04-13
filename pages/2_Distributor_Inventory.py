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

from nowadays_ui import editorial_plotly, inject_editorial_style

# Page config - MUST be first Streamlit command
st.set_page_config(
    page_title="Distributor Inventory",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)
inject_editorial_style()

# Dark mode custom CSS


COLORS = {
    'primary': '#1B4965',      # Navy (orders)
    'secondary': '#85C79D',    # Sage green (depletion)
    'success': '#85C79D',      # Sage
    'warning': '#8a6b00',      # Gold
    'danger': '#b04d5e',       # Muted red
    'info': '#5FA8D3',         # Soft blue
    'muted': '#9C9890',        # Warm grey
    'gradient': ['#1B4965', '#5FA8D3', '#85C79D', '#C4A77D']
}


def apply_dark_theme(fig, height=350, **kwargs):
    return editorial_plotly(fig, height=height, **kwargs)


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
        d.sf_account_id,
        CAST(d.total_retailers AS INT64) as total_retailers
    FROM `artful-logic-475116-p1.staging_vip.distributor_fact_sheet_2026` d
    WHERE d.distributor_code IS NOT NULL
      AND NOT d.is_parent_rollup
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
    -- SKU mapping for case-to-unit conversion
    sku_map AS (
        SELECT sf_sku, pack_size
        FROM `artful-logic-475116-p1.staging_vip.sku_mapping`
    ),

    -- VIP item master: units_per_case for UOM conversion
    vip_item_units AS (
        SELECT SupplierItem as item_code, SAFE_CAST(Units AS INT64) as units_per_case
        FROM `artful-logic-475116-p1.raw_vip.items`
        WHERE SupplierItem IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (PARTITION BY SupplierItem ORDER BY _airbyte_extracted_at DESC) = 1
    ),

    -- Salesforce orders to distributors (last N days)
    -- Excludes Draft orders
    -- Multiply by pack_size to convert cases → individual units
    sf_orders AS (
        SELECT
            sfo.account_id,
            sfo.customer_name as distributor_name,
            SUM(CAST(sfo.quantity AS INT64)) as qty_ordered_cases,
            SUM(CAST(sfo.quantity AS INT64) * COALESCE(sm.pack_size, 1)) as qty_ordered,
            SUM(CAST(sfo.line_total_price AS FLOAT64)) as order_value,
            COUNT(DISTINCT sfo.order_id) as order_count,
            MAX(sfo.order_date) as last_order_date
        FROM `artful-logic-475116-p1.staging_salesforce.salesforce_orders_flattened` sfo
        LEFT JOIN sku_map sm ON sfo.sku = sm.sf_sku
        WHERE sfo.account_type IN ('Distributor', 'Distribution Center')
            AND sfo.order_status != 'Draft'
            AND sfo.order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL {lookback_days} DAY)
            AND sfo.order_date <= CURRENT_DATE()
            AND (sfo.pricebook_name IS NULL OR (
                LOWER(sfo.pricebook_name) NOT LIKE '%sample%'
                AND LOWER(sfo.pricebook_name) NOT LIKE '%suggested%'
            ))
        GROUP BY account_id, customer_name
    ),

    -- VIP depletion by distributor (last N days)
    -- Uses raw sales_lite for fresh data
    -- Normalized to individual units using items.Units per case:
    --   UOM=C (cases): Qty × units_per_case → individual units
    --   UOM=B (bottles/cans/shots): Qty already in individual units
    vip_depletion AS (
        SELECT
            sl.Dist_Code as distributor_code,
            SUM(CASE WHEN sl.UOM = 'C' THEN SAFE_CAST(sl.Qty AS INT64) * COALESCE(iu.units_per_case, 6)
                     WHEN sl.UOM = 'B' THEN SAFE_CAST(sl.Qty AS INT64)
                     ELSE SAFE_CAST(sl.Qty AS INT64) * COALESCE(iu.units_per_case, 6) END) as qty_depleted,
            COUNT(DISTINCT sl.Acct_Code) as stores_reached,
            COUNT(*) as transaction_count,
            SUM(CASE WHEN sl.UOM = 'C' THEN SAFE_CAST(sl.Qty AS INT64) * COALESCE(iu.units_per_case, 6)
                     WHEN sl.UOM = 'B' THEN SAFE_CAST(sl.Qty AS INT64)
                     ELSE SAFE_CAST(sl.Qty AS INT64) * COALESCE(iu.units_per_case, 6) END) / ({lookback_days} / 7.0) as weekly_depletion_rate
        FROM `artful-logic-475116-p1.raw_vip.sales_lite` sl
        LEFT JOIN vip_item_units iu ON sl.Item_Code = iu.item_code
        WHERE SAFE.PARSE_DATE('%Y%m%d', sl.Invoice_Date) >= DATE_SUB(CURRENT_DATE(), INTERVAL {lookback_days} DAY)
            AND SAFE_CAST(sl.Qty AS INT64) > 0  -- Positive sales only
            AND sl.Item_Code NOT LIKE '99Z%'    -- Exclude adjustments
            AND sl.Item_Code != 'XXXXXX'        -- Exclude placeholder items
        GROUP BY sl.Dist_Code
    ),

    -- VIP distributor to SF account mapping (child accounts with VIP codes)
    -- Also get the parent account ID for rollup
    vip_to_sf AS (
        SELECT
            v.distributor_code,
            v.distributor_name as vip_dist_name,
            v.sf_account_id as sf_child_id,
            v.parent_sf_account_id as sf_parent_id,
            CAST(v.total_retailers AS INT64) as total_retailers
        FROM `artful-logic-475116-p1.staging_vip.distributor_fact_sheet_2026` v
        WHERE NOT v.is_parent_rollup
    ),

    -- Roll up VIP depletion to SF order accounts
    -- Match either: direct (order account = VIP account) OR parent (order account = parent of VIP account)
    sf_with_rollup AS (
        SELECT
            sfo.account_id,
            sfo.distributor_name,
            sfo.qty_ordered_cases,
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
        GROUP BY sfo.account_id, sfo.distributor_name, sfo.qty_ordered_cases, sfo.qty_ordered, sfo.order_value, sfo.order_count
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
            0 as qty_ordered_cases,
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

        -- Weeks of Inventory (implied inventory delta / weekly burn rate)
        CASE
            WHEN COALESCE(weekly_depletion_rate, 0) > 0
            THEN ROUND((qty_ordered - COALESCE(total_qty_depleted, 0)) / weekly_depletion_rate, 1)
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
            BrandDesc,
            SAFE_CAST(Units AS INT64) as units_per_case
        FROM `artful-logic-475116-p1.raw_vip.items`
        WHERE SupplierItem IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (PARTITION BY SupplierItem ORDER BY _airbyte_extracted_at DESC) = 1
    )
    -- Uses raw sales_lite for fresh data
    -- All quantities normalized to individual units via items.Units
    SELECT
        d.distributor_code,
        d.distributor_name,
        sl.Item_Code,
        COALESCE(i.item_description, sl.Item_Code) as product_name,
        i.BrandDesc as brand,
        SUM(CASE WHEN sl.UOM = 'C' THEN SAFE_CAST(sl.Qty AS INT64) * COALESCE(i.units_per_case, 6)
                 WHEN sl.UOM = 'B' THEN SAFE_CAST(sl.Qty AS INT64)
                 ELSE SAFE_CAST(sl.Qty AS INT64) * COALESCE(i.units_per_case, 6) END) as qty_depleted,
        COUNT(DISTINCT sl.Acct_Code) as stores_reached,
        COUNT(*) as transaction_count,
        ROUND(SUM(CASE WHEN sl.UOM = 'C' THEN SAFE_CAST(sl.Qty AS INT64) * COALESCE(i.units_per_case, 6)
                       WHEN sl.UOM = 'B' THEN SAFE_CAST(sl.Qty AS INT64)
                       ELSE SAFE_CAST(sl.Qty AS INT64) * COALESCE(i.units_per_case, 6) END) / ({lookback_days} / 7.0), 1) as weekly_depletion_rate,
        CASE
            WHEN SUM(CASE WHEN sl.UOM = 'C' THEN SAFE_CAST(sl.Qty AS INT64) * COALESCE(i.units_per_case, 6)
                          WHEN sl.UOM = 'B' THEN SAFE_CAST(sl.Qty AS INT64)
                          ELSE SAFE_CAST(sl.Qty AS INT64) * COALESCE(i.units_per_case, 6) END) / ({lookback_days} / 7.0) >= 60 THEN 'High Velocity'
            WHEN SUM(CASE WHEN sl.UOM = 'C' THEN SAFE_CAST(sl.Qty AS INT64) * COALESCE(i.units_per_case, 6)
                          WHEN sl.UOM = 'B' THEN SAFE_CAST(sl.Qty AS INT64)
                          ELSE SAFE_CAST(sl.Qty AS INT64) * COALESCE(i.units_per_case, 6) END) / ({lookback_days} / 7.0) >= 18 THEN 'Medium Velocity'
            ELSE 'Low Velocity'
        END as velocity_status
    FROM `artful-logic-475116-p1.raw_vip.sales_lite` sl
    JOIN `artful-logic-475116-p1.staging_vip.distributor_fact_sheet_2026` d
        ON sl.Dist_Code = d.distributor_code
    LEFT JOIN items_deduped i
        ON sl.Item_Code = i.item_code
    WHERE SAFE.PARSE_DATE('%Y%m%d', sl.Invoice_Date) >= DATE_SUB(CURRENT_DATE(), INTERVAL {lookback_days} DAY)
        AND SAFE_CAST(sl.Qty AS INT64) > 0  -- Positive sales only
        AND sl.Item_Code NOT LIKE '99Z%'    -- Exclude adjustments
        AND sl.Item_Code != 'XXXXXX'        -- Exclude placeholder items
        {distributor_filter}
    GROUP BY d.distributor_code, d.distributor_name, sl.Item_Code, i.item_description, i.BrandDesc
    ORDER BY d.distributor_name, qty_depleted DESC
    """
    return client.query(query).to_dataframe()


@st.cache_data(ttl=300)
def load_state_depletion_data(lookback_days: int = 90):
    """Load depletion data aggregated by state for US map visualization."""
    client = get_bq_client()

    query = f"""
    -- Uses raw sales_lite for fresh data
    -- POD = Points of Distribution = unique (door, SKU) combinations
    -- All quantities normalized to individual units via items.Units
    WITH vip_item_units AS (
        SELECT SupplierItem as item_code, SAFE_CAST(Units AS INT64) as units_per_case
        FROM `artful-logic-475116-p1.raw_vip.items`
        WHERE SupplierItem IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (PARTITION BY SupplierItem ORDER BY _airbyte_extracted_at DESC) = 1
    ),
    state_depletion AS (
        SELECT
            d.State as state,
            CAST(d.Distributor_ID AS STRING) as distributor_code,
            SUM(CASE WHEN sl.UOM = 'C' THEN SAFE_CAST(sl.Qty AS INT64) * COALESCE(iu.units_per_case, 6)
                     WHEN sl.UOM = 'B' THEN SAFE_CAST(sl.Qty AS INT64)
                     ELSE SAFE_CAST(sl.Qty AS INT64) * COALESCE(iu.units_per_case, 6) END) as qty_depleted,
            COUNT(DISTINCT sl.Acct_Code) as stores_reached,
            COUNT(DISTINCT CONCAT(sl.Acct_Code, '|', sl.Item_Code)) as pod_count
        FROM `artful-logic-475116-p1.raw_vip.sales_lite` sl
        JOIN `artful-logic-475116-p1.raw_vip.distributors` d
            ON sl.Dist_Code = CAST(d.Distributor_ID AS STRING)
        LEFT JOIN vip_item_units iu ON sl.Item_Code = iu.item_code
        WHERE SAFE.PARSE_DATE('%Y%m%d', sl.Invoice_Date) >= DATE_SUB(CURRENT_DATE(), INTERVAL {lookback_days} DAY)
            AND SAFE_CAST(sl.Qty AS INT64) > 0  -- Positive sales only
            AND sl.Item_Code NOT LIKE '99Z%'    -- Exclude adjustments
            AND sl.Item_Code != 'XXXXXX'        -- Exclude placeholder items
            AND d.State IS NOT NULL
            AND LENGTH(d.State) = 2
        GROUP BY d.State, d.Distributor_ID
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
    -- SKU mapping for case-to-unit conversion
    sku_map AS (
        SELECT sf_sku, pack_size
        FROM `artful-logic-475116-p1.staging_vip.sku_mapping`
    ),

    -- Weekly SF orders (excludes Draft orders)
    -- IMPORTANT: Multiply by pack_size to convert cases → units
    weekly_orders AS (
        SELECT
            DATE_TRUNC(sfo.order_date, WEEK) as week_start,
            SUM(CAST(sfo.quantity AS INT64)) as qty_ordered_cases,
            SUM(CAST(sfo.quantity AS INT64) * COALESCE(sm.pack_size, 1)) as qty_ordered,
            SUM(CAST(sfo.line_total_price AS FLOAT64)) as order_value,
            COUNT(DISTINCT sfo.order_id) as order_count
        FROM `artful-logic-475116-p1.staging_salesforce.salesforce_orders_flattened` sfo
        LEFT JOIN sku_map sm ON sfo.sku = sm.sf_sku
        WHERE sfo.account_type IN ('Distributor', 'Distribution Center')
            AND sfo.order_status != 'Draft'
            AND sfo.order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL {lookback_weeks} WEEK)
            AND sfo.order_date <= CURRENT_DATE()
            AND (sfo.pricebook_name IS NULL OR (
                LOWER(sfo.pricebook_name) NOT LIKE '%sample%'
                AND LOWER(sfo.pricebook_name) NOT LIKE '%suggested%'
            ))
        GROUP BY week_start
    ),

    -- VIP item master: units_per_case for UOM conversion
    vip_item_units AS (
        SELECT SupplierItem as item_code, SAFE_CAST(Units AS INT64) as units_per_case
        FROM `artful-logic-475116-p1.raw_vip.items`
        WHERE SupplierItem IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (PARTITION BY SupplierItem ORDER BY _airbyte_extracted_at DESC) = 1
    ),

    -- Weekly VIP depletion (uses raw sales_lite for fresh data)
    -- Normalized to individual units via items.Units
    weekly_depletion AS (
        SELECT
            DATE_TRUNC(SAFE.PARSE_DATE('%Y%m%d', sl.Invoice_Date), WEEK) as week_start,
            SUM(CASE WHEN sl.UOM = 'C' THEN SAFE_CAST(sl.Qty AS INT64) * COALESCE(iu.units_per_case, 6)
                     WHEN sl.UOM = 'B' THEN SAFE_CAST(sl.Qty AS INT64)
                     ELSE SAFE_CAST(sl.Qty AS INT64) * COALESCE(iu.units_per_case, 6) END) as qty_depleted,
            COUNT(DISTINCT sl.Acct_Code) as stores_reached
        FROM `artful-logic-475116-p1.raw_vip.sales_lite` sl
        LEFT JOIN vip_item_units iu ON sl.Item_Code = iu.item_code
        WHERE SAFE.PARSE_DATE('%Y%m%d', sl.Invoice_Date) >= DATE_SUB(CURRENT_DATE(), INTERVAL {lookback_weeks} WEEK)
            AND SAFE_CAST(sl.Qty AS INT64) > 0  -- Positive sales only
            AND sl.Item_Code NOT LIKE '99Z%'    -- Exclude adjustments
            AND sl.Item_Code != 'XXXXXX'        -- Exclude placeholder items
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


@st.cache_data(ttl=300)
def load_trend_by_family(lookback_weeks: int = 12):
    """Load weekly depletion trends broken out by product family (Cans, Bottles, Shots)."""
    client = get_bq_client()

    query = f"""
    WITH vip_item_units AS (
        SELECT SupplierItem as item_code,
            SAFE_CAST(Units AS INT64) as units_per_case,
            CASE
                WHEN `Desc` LIKE '%Seltzer%' OR `Desc` LIKE '%oz' AND SAFE_CAST(Units AS INT64) = 24 THEN 'Cans'
                WHEN `Desc` LIKE '%750%' OR `Desc` LIKE '%ml' THEN 'Bottles'
                WHEN `Desc` LIKE '%Shot%' OR `Desc` LIKE '%2 oz%' THEN 'Shots'
                ELSE 'Other'
            END as product_family
        FROM `artful-logic-475116-p1.raw_vip.items`
        WHERE SupplierItem IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (PARTITION BY SupplierItem ORDER BY _airbyte_extracted_at DESC) = 1
    )
    SELECT
        DATE_TRUNC(SAFE.PARSE_DATE('%Y%m%d', sl.Invoice_Date), WEEK) as week_start,
        COALESCE(iu.product_family, 'Other') as product_family,
        SUM(CASE WHEN sl.UOM = 'C' THEN SAFE_CAST(sl.Qty AS INT64) * COALESCE(iu.units_per_case, 6)
                 WHEN sl.UOM = 'B' THEN SAFE_CAST(sl.Qty AS INT64)
                 ELSE SAFE_CAST(sl.Qty AS INT64) * COALESCE(iu.units_per_case, 6) END) as qty_depleted,
        COUNT(DISTINCT sl.Acct_Code) as stores_reached
    FROM `artful-logic-475116-p1.raw_vip.sales_lite` sl
    LEFT JOIN vip_item_units iu ON sl.Item_Code = iu.item_code
    WHERE SAFE.PARSE_DATE('%Y%m%d', sl.Invoice_Date) >= DATE_SUB(CURRENT_DATE(), INTERVAL {lookback_weeks} WEEK)
        AND SAFE_CAST(sl.Qty AS INT64) > 0
        AND sl.Item_Code NOT LIKE '99Z%'
        AND sl.Item_Code != 'XXXXXX'
    GROUP BY week_start, product_family
    ORDER BY week_start, product_family
    """
    return client.query(query).to_dataframe()


@st.cache_data(ttl=300)
def load_distributor_weekly_trends(lookback_weeks: int = 12):
    """Load weekly depletion trends per distributor for granular forecasting."""
    client = get_bq_client()

    query = f"""
    WITH vip_item_units AS (
        SELECT SupplierItem as item_code, SAFE_CAST(Units AS INT64) as units_per_case
        FROM `artful-logic-475116-p1.raw_vip.items`
        WHERE SupplierItem IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (PARTITION BY SupplierItem ORDER BY _airbyte_extracted_at DESC) = 1
    ),
    weekly_by_dist AS (
        SELECT
            d.distributor_code,
            d.distributor_name,
            DATE_TRUNC(SAFE.PARSE_DATE('%Y%m%d', sl.Invoice_Date), WEEK) as week_start,
            SUM(CASE WHEN sl.UOM = 'C' THEN SAFE_CAST(sl.Qty AS INT64) * COALESCE(iu.units_per_case, 6)
                     WHEN sl.UOM = 'B' THEN SAFE_CAST(sl.Qty AS INT64)
                     ELSE SAFE_CAST(sl.Qty AS INT64) * COALESCE(iu.units_per_case, 6) END) as qty_depleted,
            COUNT(DISTINCT sl.Acct_Code) as stores_reached,
            COUNT(DISTINCT sl.Item_Code) as skus_sold
        FROM `artful-logic-475116-p1.raw_vip.sales_lite` sl
        JOIN `artful-logic-475116-p1.staging_vip.distributor_fact_sheet_2026` d
            ON sl.Dist_Code = d.distributor_code
            AND NOT d.is_parent_rollup
        LEFT JOIN vip_item_units iu ON sl.Item_Code = iu.item_code
        WHERE SAFE.PARSE_DATE('%Y%m%d', sl.Invoice_Date) >= DATE_SUB(CURRENT_DATE(), INTERVAL {lookback_weeks} WEEK)
            AND SAFE_CAST(sl.Qty AS INT64) > 0  -- Positive sales only
            AND sl.Item_Code NOT LIKE '99Z%'    -- Exclude adjustments
            AND sl.Item_Code != 'XXXXXX'        -- Exclude placeholder items
        GROUP BY d.distributor_code, d.distributor_name, week_start
    )
    SELECT
        distributor_code,
        distributor_name,
        week_start,
        qty_depleted,
        stores_reached,
        skus_sold,
        AVG(qty_depleted) OVER (
            PARTITION BY distributor_code
            ORDER BY week_start
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) as ma_4wk,
        LAG(qty_depleted, 4) OVER (
            PARTITION BY distributor_code ORDER BY week_start
        ) as qty_4wk_ago
    FROM weekly_by_dist
    ORDER BY distributor_code, week_start
    """
    return client.query(query).to_dataframe()


@st.cache_data(ttl=300)
def load_woi_by_sku(distributor_ids: list = None):
    """Load WOI by parent distributor x SKU from pre-computed mart table.

    Aggregated at parent distributor level (~50 distributors) not individual DCs.
    """
    client = get_bq_client()

    distributor_filter = ""
    if distributor_ids and len(distributor_ids) > 0:
        ids_str = "', '".join(distributor_ids)
        distributor_filter = f"WHERE distributor_id IN ('{ids_str}')"

    query = f"""
    SELECT
        distributor_id,
        distributor_name,
        vip_item_code,
        product_name,
        product_category,
        qty_ordered_cases,
        qty_ordered,
        order_value,
        qty_depleted,
        weekly_depletion_rate,
        weeks_of_inventory,
        inventory_status,
        velocity_tier,
        order_depletion_ratio,
        implied_inventory_delta,
        dc_count,
        last_order_date,
        last_depletion_date
    FROM `artful-logic-475116-p1.staging_vip.woi_by_distro_sku`
    {distributor_filter}
    ORDER BY distributor_name, qty_depleted DESC
    """
    return client.query(query).to_dataframe()


def calculate_stockout_risk(inventory_df: pd.DataFrame, dist_trends_df: pd.DataFrame):
    """
    Calculate stockout risk for each distributor based on:
    - Current weeks of inventory
    - Depletion velocity trend (accelerating/decelerating)
    - Velocity consistency (coefficient of variation)

    Returns dataframe with stockout predictions and risk scores.
    """
    if inventory_df.empty:
        return pd.DataFrame()

    results = []
    today = datetime.now().date()

    for _, row in inventory_df.iterrows():
        dist_code = row.get('distributor_code') or row.get('vip_codes', '').split(',')[0].strip()
        dist_name = row['distributor_name']
        weeks_inv = row.get('weeks_of_inventory')
        weekly_rate = row.get('weekly_depletion_rate', 0) or 0
        qty_ordered = row.get('total_qty_ordered', 0) or 0
        qty_depleted = row.get('total_qty_depleted', 0) or 0

        # Get distributor-specific trend data
        dist_data = dist_trends_df[dist_trends_df['distributor_code'] == dist_code] if not dist_trends_df.empty else pd.DataFrame()

        # Calculate velocity trend
        velocity_trend = 0
        velocity_cv = 0.2  # default coefficient of variation

        if len(dist_data) >= 4:
            recent_4wk = dist_data.tail(4)['qty_depleted'].mean()
            prev_4wk = dist_data.head(4)['qty_depleted'].mean() if len(dist_data) >= 8 else recent_4wk
            if prev_4wk > 0:
                velocity_trend = (recent_4wk - prev_4wk) / prev_4wk

            # Velocity consistency
            if dist_data['qty_depleted'].mean() > 0:
                velocity_cv = dist_data['qty_depleted'].std() / dist_data['qty_depleted'].mean()

        # Adjusted weekly rate based on trend
        trend_factor = 1 + (velocity_trend * 0.5)  # Dampen trend impact
        adjusted_weekly_rate = weekly_rate * trend_factor if weekly_rate > 0 else 0

        # Stockout prediction
        if adjusted_weekly_rate > 0 and qty_ordered > 0:
            weeks_until_stockout = qty_ordered / adjusted_weekly_rate
            stockout_date = today + timedelta(weeks=weeks_until_stockout)
        elif weeks_inv and weeks_inv > 0:
            weeks_until_stockout = weeks_inv
            stockout_date = today + timedelta(weeks=weeks_inv)
        else:
            weeks_until_stockout = None
            stockout_date = None

        # Risk score (0-100)
        # Higher risk = lower weeks, accelerating velocity, inconsistent velocity
        if weeks_until_stockout is not None:
            base_risk = max(0, min(100, 100 - (weeks_until_stockout * 8)))  # 0 weeks = 100, 12+ weeks = 0
            trend_modifier = 1 + max(0, velocity_trend * 0.3)  # Accelerating = higher risk
            consistency_modifier = 1 + min(0.3, velocity_cv * 0.5)  # Inconsistent = higher risk
            risk_score = min(100, base_risk * trend_modifier * consistency_modifier)
        else:
            risk_score = 50 if qty_depleted > 0 else 0  # Unknown = medium risk if active

        # Reorder recommendation
        target_weeks = 8  # Target 8 weeks of inventory
        if adjusted_weekly_rate > 0:
            target_qty = adjusted_weekly_rate * target_weeks
            current_equiv_qty = qty_ordered
            reorder_qty = max(0, target_qty - current_equiv_qty)

            # Urgency based on weeks until stockout
            if weeks_until_stockout is not None and weeks_until_stockout < 3:
                urgency = 'CRITICAL'
            elif weeks_until_stockout is not None and weeks_until_stockout < 6:
                urgency = 'HIGH'
            elif weeks_until_stockout is not None and weeks_until_stockout < 10:
                urgency = 'MEDIUM'
            else:
                urgency = 'LOW'
        else:
            reorder_qty = 0
            urgency = 'N/A'

        results.append({
            'distributor_code': dist_code,
            'distributor_name': dist_name,
            'current_inventory_weeks': weeks_inv,
            'weekly_depletion_rate': weekly_rate,
            'adjusted_weekly_rate': adjusted_weekly_rate,
            'velocity_trend_pct': velocity_trend * 100,
            'velocity_cv': velocity_cv,
            'weeks_until_stockout': weeks_until_stockout,
            'predicted_stockout_date': stockout_date,
            'risk_score': risk_score,
            'reorder_qty_suggested': reorder_qty,
            'reorder_urgency': urgency,
            'qty_ordered_period': qty_ordered,
            'qty_depleted_period': qty_depleted
        })

    return pd.DataFrame(results)


def generate_pipeline_forecast(stockout_df: pd.DataFrame, forecast_weeks: int = 12):
    """
    Generate pipeline-ready forecast data for integration with external systems.
    Returns weekly projections per distributor.
    """
    if stockout_df.empty:
        return pd.DataFrame()

    today = datetime.now().date()
    future_weeks = [today + timedelta(weeks=i) for i in range(1, forecast_weeks + 1)]

    pipeline_rows = []

    for _, row in stockout_df.iterrows():
        dist_code = row['distributor_code']
        dist_name = row['distributor_name']
        weekly_rate = row.get('adjusted_weekly_rate', 0) or 0
        current_inv = row.get('qty_ordered_period', 0) or 0

        running_inventory = current_inv

        for i, week_date in enumerate(future_weeks):
            # Simple projection: current inventory minus cumulative depletion
            projected_depletion = weekly_rate
            running_inventory = max(0, running_inventory - projected_depletion)

            # Stockout flag
            is_stockout = running_inventory <= 0

            pipeline_rows.append({
                'distributor_code': dist_code,
                'distributor_name': dist_name,
                'forecast_week': week_date,
                'week_number': i + 1,
                'projected_depletion': projected_depletion,
                'projected_inventory': running_inventory,
                'is_stockout': is_stockout,
                'weekly_depletion_rate': weekly_rate
            })

    return pd.DataFrame(pipeline_rows)


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

    # Filter Section - selectors first, then data load uses their values
    col1, col2, col_uom, col3, col4 = st.columns([3, 1, 1, 1, 1])

    with col2:
        lookback_days = st.selectbox(
            "Lookback Period",
            options=[30, 60, 90, 180],
            index=2,
            format_func=lambda x: f"{x} days"
        )

    with col_uom:
        unit_mode = st.selectbox(
            "Unit of Measure",
            options=['Units', 'Cases'],
            index=0,
            help="Units = individual bottles/cans/shots. Cases = distributor selling units."
        )

    # Per-family case divisors for converting individual units → cases
    FAMILY_CASE_SIZE = {'Cans': 24, 'Bottles': 6, 'Shots': 36, 'Other': 6}

    with col3:
        understock_threshold = st.selectbox(
            "Understock Threshold",
            options=[2, 3, 4, 6],
            index=2,
            format_func=lambda x: f"{x} weeks"
        )

    with col4:
        overstock_threshold = st.selectbox(
            "Overstock Threshold",
            options=[8, 10, 12, 16],
            index=2,
            format_func=lambda x: f"{x} weeks"
        )

    # Load data with selected lookback period
    try:
        distributors_df = load_distributors()
        inventory_df = load_inventory_data(lookback_days=lookback_days)
        lookback_weeks = lookback_days // 7
        trend_df = load_trend_data(lookback_weeks=lookback_weeks)
        family_trend_df = load_trend_by_family(lookback_weeks=lookback_weeks)
        # Load distributor-level trends for stockout analysis
        dist_trends_df = load_distributor_weekly_trends(lookback_weeks=12)
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return

    with col1:
        # Distributor multiselect (after data load so we have the options)
        distributor_options = ["All Distributors"] + sorted(inventory_df['distributor_name'].dropna().unique().tolist())
        selected_distributors = st.multiselect(
            "Select Distributors",
            options=distributor_options,
            default=["All Distributors"],
            help="Select one or more distributors to filter the analysis"
        )

    # Filter the data based on selection
    if "All Distributors" not in selected_distributors and len(selected_distributors) > 0:
        filtered_df = inventory_df[inventory_df['distributor_name'].isin(selected_distributors)]
    else:
        filtered_df = inventory_df.copy()

    # Unit display helpers
    uom_label = 'Cases' if unit_mode == 'Cases' else 'Units'
    # Weighted avg pack size from the product family mix for aggregate conversion
    if not family_trend_df.empty and unit_mode == 'Cases':
        _fam_totals = family_trend_df.groupby('product_family')['qty_depleted'].sum()
        _weighted = sum(_fam_totals.get(f, 0) / FAMILY_CASE_SIZE.get(f, 6) for f in _fam_totals.index)
        avg_case_divisor = _fam_totals.sum() / _weighted if _weighted > 0 else 6
    else:
        avg_case_divisor = 1
    def to_display_units(val):
        """Convert individual units to display units (cases or units) based on toggle."""
        return val / avg_case_divisor if unit_mode == 'Cases' else val

    # Calculate summary stats
    total_distributors = len(filtered_df)
    total_order_value = filtered_df['total_order_value'].sum()
    total_qty_ordered = filtered_df['total_qty_ordered'].sum()
    total_qty_depleted = filtered_df['total_qty_depleted'].sum()

    # Separate distributors with and without depletion data
    has_depletion_df = filtered_df[
        (filtered_df['total_qty_depleted'].notna()) &
        (filtered_df['total_qty_depleted'] > 0)
    ]

    # Status counts based on weeks_of_inventory (using selected thresholds)
    # <understock_threshold = Understock, between = Balanced, >overstock_threshold = Overstock
    overstock_count = len(has_depletion_df[has_depletion_df['weeks_of_inventory'] > overstock_threshold])
    understock_count = len(has_depletion_df[has_depletion_df['weeks_of_inventory'] < understock_threshold])
    balanced_count = len(has_depletion_df[
        (has_depletion_df['weeks_of_inventory'] >= understock_threshold) &
        (has_depletion_df['weeks_of_inventory'] <= overstock_threshold)
    ])
    no_depletion_count = len(filtered_df) - len(has_depletion_df)

    avg_weeks = has_depletion_df[has_depletion_df['weeks_of_inventory'].notna() & (has_depletion_df['weeks_of_inventory'] > 0)]['weeks_of_inventory'].mean()

    # KPI Cards Row 1: Volume & Value
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.markdown(render_metric_card(
            f"{total_distributors:,}",
            "Active Distributors"
        ), unsafe_allow_html=True)

    with col2:
        st.markdown(render_metric_card(
            f"${total_order_value/1000000:.1f}M",
            f"Order Value ({lookback_days}d)"
        ), unsafe_allow_html=True)

    with col3:
        st.markdown(render_metric_card(
            f"{to_display_units(total_qty_ordered):,.0f}",
            f"{uom_label} Ordered ({lookback_days}d)"
        ), unsafe_allow_html=True)

    with col4:
        st.markdown(render_metric_card(
            f"{to_display_units(total_qty_depleted):,.0f}",
            f"{uom_label} Depleted ({lookback_days}d)"
        ), unsafe_allow_html=True)

    # KPI Cards Row 2: Inventory Health
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        has_depletion_total = len(has_depletion_df)
        overstock_pct = round(100 * overstock_count / max(has_depletion_total, 1), 1)
        st.markdown(render_metric_card(
            f"{overstock_count} ({overstock_pct}%)",
            f"Overstocked (>{overstock_threshold} wks)",
            card_type="warning"
        ), unsafe_allow_html=True)

    with col2:
        understock_pct = round(100 * understock_count / max(has_depletion_total, 1), 1)
        st.markdown(render_metric_card(
            f"{understock_count} ({understock_pct}%)",
            f"Understocked (<{understock_threshold} wks)",
            card_type="danger"
        ), unsafe_allow_html=True)

    with col3:
        avg_weeks_display = f"{avg_weeks:.1f}" if pd.notna(avg_weeks) else "N/A"
        st.markdown(render_metric_card(
            avg_weeks_display,
            "Avg Weeks Inventory"
        ), unsafe_allow_html=True)

    with col4:
        od_ratio = round(total_qty_ordered / total_qty_depleted, 2) if total_qty_depleted > 0 else 0
        od_type = "primary" if 0.8 <= od_ratio <= 1.3 else ("warning" if od_ratio > 1.3 else "danger")
        st.markdown(render_metric_card(
            f"{od_ratio:.2f}",
            "Order/Depletion Ratio",
            card_type=od_type
        ), unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # Charts Row 1: Trend + Status Distribution
    col1, col2 = st.columns([2, 1])

    with col1:
        st.markdown(f'<p class="section-header">{uom_label} Ordered vs Depleted</p>', unsafe_allow_html=True)

        if not trend_df.empty:
            trend_sorted = trend_df.sort_values('week_start').copy()

            # Apply unit conversion for display
            trend_sorted['qty_ordered_display'] = trend_sorted['qty_ordered'].apply(to_display_units)
            trend_sorted['qty_depleted_display'] = trend_sorted['qty_depleted'].apply(to_display_units)
            trend_sorted['orders_ma'] = trend_sorted['qty_ordered_display'].rolling(window=4, min_periods=2).mean()
            trend_sorted['depletion_ma'] = trend_sorted['qty_depleted_display'].rolling(window=4, min_periods=2).mean()

            fig = go.Figure()

            fig.add_trace(go.Scatter(
                x=trend_sorted['week_start'],
                y=trend_sorted['qty_ordered_display'],
                mode='lines+markers',
                name=f'{uom_label} Ordered (SF)',
                line=dict(color=COLORS['primary'], width=2),
                marker=dict(size=6),
                opacity=0.7
            ))

            fig.add_trace(go.Scatter(
                x=trend_sorted['week_start'],
                y=trend_sorted['qty_depleted_display'],
                mode='lines+markers',
                name=f'{uom_label} Depleted (VIP)',
                line=dict(color=COLORS['secondary'], width=2),
                marker=dict(size=6),
                opacity=0.7
            ))

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
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font=dict(color='#2D2926')),
                hovermode='x unified',
                yaxis=dict(title=dict(text=uom_label, font=dict(color='#2D2926')), tickfont=dict(color='#625f56'), tickformat=',.0f')
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No trend data available")

    with col2:
        st.markdown('<p class="section-header">Distributor Inventory Health</p>', unsafe_allow_html=True)

        # Reuse the counts calculated above for consistency
        # understock_count, balanced_count, overstock_count already calculated from has_depletion_df
        understock_count_chart = understock_count
        balanced_count_chart = balanced_count
        overstock_count_chart = overstock_count
        no_depletion_count_chart = no_depletion_count

        fig = go.Figure()

        # Stacked bar: With Depletion Data (broken down by health)
        fig.add_trace(go.Bar(
            name=f'Understock (<{understock_threshold} wks)',
            x=['With Depletion'],
            y=[understock_count_chart],
            marker_color=COLORS['danger'],
            text=[understock_count_chart] if understock_count_chart > 0 else None,
            textposition='inside',
            textfont=dict(color='#2D2926', size=13, family='Jost')
        ))

        fig.add_trace(go.Bar(
            name=f'Balanced ({understock_threshold}-{overstock_threshold} wks)',
            x=['With Depletion'],
            y=[balanced_count_chart],
            marker_color=COLORS['success'],
            text=[balanced_count_chart] if balanced_count_chart > 0 else None,
            textposition='inside',
            textfont=dict(color='#2D2926', size=13, family='Jost')
        ))

        fig.add_trace(go.Bar(
            name=f'Overstock (>{overstock_threshold} wks)',
            x=['With Depletion'],
            y=[overstock_count_chart],
            marker_color=COLORS['warning'],
            text=[overstock_count_chart] if overstock_count_chart > 0 else None,
            textposition='inside',
            textfont=dict(color='#2D2926', size=13, family='Jost')
        ))

        # Second bar: No Depletion Data
        fig.add_trace(go.Bar(
            name='No Depletion Data',
            x=['No Depletion'],
            y=[no_depletion_count_chart],
            marker_color=COLORS['muted'],
            text=[no_depletion_count_chart] if no_depletion_count_chart > 0 else None,
            textposition='inside',
            textfont=dict(color='#2D2926', size=13, family='Jost'),
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
                font=dict(color='#2D2926', size=10)
            ),
            xaxis=dict(tickfont=dict(color='#625f56'))
        )
        st.plotly_chart(fig, use_container_width=True)

    # Volume by Product Family (Cans, Bottles, Shots)
    st.markdown('<p class="section-header">Depletion by Product Family</p>', unsafe_allow_html=True)

    if not family_trend_df.empty:
        family_colors = {'Cans': COLORS['primary'], 'Bottles': COLORS['secondary'],
                         'Shots': COLORS['info'], 'Other': COLORS['muted']}

        # Stacked area chart
        fam_col1, fam_col2 = st.columns([2, 1])

        with fam_col1:
            fig_fam = go.Figure()
            for family in ['Cans', 'Bottles', 'Shots', 'Other']:
                fam_data = family_trend_df[family_trend_df['product_family'] == family].sort_values('week_start')
                if not fam_data.empty:
                    fam_divisor = FAMILY_CASE_SIZE.get(family, 6) if unit_mode == 'Cases' else 1
                    display_qty = fam_data['qty_depleted'] / fam_divisor
                    fig_fam.add_trace(go.Scatter(
                        x=fam_data['week_start'],
                        y=display_qty,
                        mode='lines',
                        name=family,
                        stackgroup='one',
                        line=dict(width=0.5, color=family_colors.get(family, COLORS['muted'])),
                        hovertemplate=f'{family}: %{{y:,.0f}} {uom_label.lower()}<extra></extra>'
                    ))
            apply_dark_theme(fig_fam, height=300,
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font=dict(color='#2D2926')),
                hovermode='x unified',
                yaxis=dict(title=dict(text=f'{uom_label} Depleted'), tickformat=',.0f')
            )
            st.plotly_chart(fig_fam, use_container_width=True)

        with fam_col2:
            # Period totals by family (with per-family case conversion)
            family_totals = family_trend_df.groupby('product_family')['qty_depleted'].sum().sort_values(ascending=False)
            grand_total = family_totals.sum()
            for family, total in family_totals.items():
                fam_divisor = FAMILY_CASE_SIZE.get(family, 6) if unit_mode == 'Cases' else 1
                display_total = total / fam_divisor
                pct = round(100 * total / grand_total, 1) if grand_total > 0 else 0
                st.markdown(render_metric_card(
                    f"{display_total:,.0f}",
                    f"{family} ({pct}%)"
                ), unsafe_allow_html=True)

    # ==================== STOCKOUT RISK & PIPELINE FORECAST ====================
    st.markdown('<p class="section-header">🚨 Stockout Risk & Reorder Recommendations</p>', unsafe_allow_html=True)
    st.markdown('<p class="dashboard-subtitle" style="margin-top: -10px;">Per-distributor stockout predictions with velocity-adjusted forecasting for pipeline planning</p>', unsafe_allow_html=True)

    # Calculate stockout risk for filtered distributors
    stockout_df = calculate_stockout_risk(filtered_df, dist_trends_df)

    if not stockout_df.empty:
        # KPI Row: Risk Summary
        critical_count = len(stockout_df[stockout_df['reorder_urgency'] == 'CRITICAL'])
        high_count = len(stockout_df[stockout_df['reorder_urgency'] == 'HIGH'])
        medium_count = len(stockout_df[stockout_df['reorder_urgency'] == 'MEDIUM'])
        avg_risk = stockout_df['risk_score'].mean()
        total_reorder_qty = stockout_df['reorder_qty_suggested'].sum()

        rcol1, rcol2, rcol3, rcol4, rcol5 = st.columns(5)
        with rcol1:
            st.markdown(render_metric_card(f"{critical_count}", "Critical Risk", "danger"), unsafe_allow_html=True)
        with rcol2:
            st.markdown(render_metric_card(f"{high_count}", "High Risk", "warning"), unsafe_allow_html=True)
        with rcol3:
            st.markdown(render_metric_card(f"{medium_count}", "Medium Risk", "primary"), unsafe_allow_html=True)
        with rcol4:
            st.markdown(render_metric_card(f"{avg_risk:.0f}", "Avg Risk Score", "primary"), unsafe_allow_html=True)
        with rcol5:
            st.markdown(render_metric_card(f"{total_reorder_qty:,.0f}", "Total Reorder Qty", "primary"), unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)

        # Two columns: Stockout Timeline + Risk Distribution
        scol1, scol2 = st.columns(2)

        with scol1:
            st.markdown("**Predicted Stockout Timeline**")
            # Filter to distributors with valid stockout dates
            timeline_df = stockout_df[stockout_df['predicted_stockout_date'].notna()].copy()
            timeline_df = timeline_df.sort_values('weeks_until_stockout').head(15)

            if not timeline_df.empty:
                # Color by urgency
                urgency_colors = {
                    'CRITICAL': COLORS['danger'],
                    'HIGH': COLORS['warning'],
                    'MEDIUM': COLORS['info'],
                    'LOW': COLORS['success'],
                    'N/A': COLORS['muted']
                }
                bar_colors = [urgency_colors.get(u, COLORS['muted']) for u in timeline_df['reorder_urgency']]

                fig = go.Figure(go.Bar(
                    x=timeline_df['weeks_until_stockout'],
                    y=timeline_df['distributor_name'],
                    orientation='h',
                    marker=dict(color=bar_colors),
                    text=timeline_df.apply(lambda r: f"{r['weeks_until_stockout']:.1f} wks ({r['reorder_urgency']})", axis=1),
                    textposition='outside',
                    textfont=dict(color='#2D2926', size=10),
                    hovertemplate='%{y}<br>Weeks to Stockout: %{x:.1f}<br>Stockout Date: %{customdata}<extra></extra>',
                    customdata=timeline_df['predicted_stockout_date'].apply(lambda d: d.strftime('%b %d') if d else 'N/A')
                ))
                apply_dark_theme(fig, height=400, margin=dict(l=0, r=80, t=10, b=0), yaxis={'autorange': 'reversed'},
                    xaxis={'title': 'Weeks Until Stockout'})
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No distributors with predictable stockout dates")

        with scol2:
            st.markdown("**Risk Score Distribution**")
            # Scatter plot: Risk Score vs Weeks Until Stockout
            scatter_df = stockout_df[stockout_df['weeks_until_stockout'].notna()].copy()

            if not scatter_df.empty:
                fig = go.Figure()

                # Add scatter points colored by urgency
                for urgency in ['CRITICAL', 'HIGH', 'MEDIUM', 'LOW']:
                    urg_data = scatter_df[scatter_df['reorder_urgency'] == urgency]
                    if not urg_data.empty:
                        fig.add_trace(go.Scatter(
                            x=urg_data['weeks_until_stockout'],
                            y=urg_data['risk_score'],
                            mode='markers',
                            name=urgency,
                            marker=dict(
                                size=10,
                                color={'CRITICAL': COLORS['danger'], 'HIGH': COLORS['warning'],
                                       'MEDIUM': COLORS['info'], 'LOW': COLORS['success']}[urgency]
                            ),
                            text=urg_data['distributor_name'],
                            hovertemplate='%{text}<br>Weeks: %{x:.1f}<br>Risk: %{y:.0f}<extra></extra>'
                        ))

                # Add danger zone shading
                fig.add_hrect(y0=70, y1=100, fillcolor="rgba(255,107,107,0.1)", line_width=0)
                fig.add_vrect(x0=0, x1=4, fillcolor="rgba(255,107,107,0.1)", line_width=0)

                apply_dark_theme(fig, height=400,
                    xaxis={'title': 'Weeks Until Stockout'},
                    yaxis={'title': 'Risk Score (0-100)'},
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font=dict(color='#2D2926')))
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No risk data available")

        # Reorder Recommendations Table
        st.markdown("**📋 Reorder Recommendations (Pipeline-Ready)**")

        reorder_display = stockout_df[[
            'distributor_name', 'reorder_urgency', 'weeks_until_stockout', 'predicted_stockout_date',
            'reorder_qty_suggested', 'weekly_depletion_rate', 'velocity_trend_pct', 'risk_score'
        ]].copy()

        # Format columns
        reorder_display['weeks_until_stockout'] = reorder_display['weeks_until_stockout'].apply(
            lambda x: f"{x:.1f}" if pd.notna(x) else "N/A")
        reorder_display['predicted_stockout_date'] = reorder_display['predicted_stockout_date'].apply(
            lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) else "N/A")
        reorder_display['reorder_qty_suggested'] = reorder_display['reorder_qty_suggested'].apply(
            lambda x: f"{x:,.0f}" if x > 0 else "-")
        reorder_display['weekly_depletion_rate'] = reorder_display['weekly_depletion_rate'].apply(
            lambda x: f"{x:,.0f}/wk" if pd.notna(x) and x > 0 else "-")
        reorder_display['velocity_trend_pct'] = reorder_display['velocity_trend_pct'].apply(
            lambda x: f"{x:+.1f}%" if pd.notna(x) else "-")
        reorder_display['risk_score'] = reorder_display['risk_score'].apply(
            lambda x: f"{x:.0f}" if pd.notna(x) else "-")

        reorder_display.columns = ['Distributor', 'Urgency', 'Weeks to Stockout', 'Stockout Date',
                                   'Suggested Reorder Qty', 'Weekly Velocity', 'Velocity Trend', 'Risk Score']

        # Sort by urgency priority
        urgency_order = {'CRITICAL': 0, 'HIGH': 1, 'MEDIUM': 2, 'LOW': 3, 'N/A': 4}
        reorder_display['_sort'] = reorder_display['Urgency'].map(urgency_order)
        reorder_display = reorder_display.sort_values('_sort').drop('_sort', axis=1)

        st.dataframe(reorder_display, use_container_width=True, hide_index=True, height=350)

        # Pipeline Export Section
        with st.expander("📤 Pipeline Forecast Data (12-Week Projection)", expanded=False):
            pipeline_df = generate_pipeline_forecast(stockout_df, forecast_weeks=12)
            if not pipeline_df.empty:
                # Summary by week
                weekly_summary = pipeline_df.groupby('forecast_week').agg({
                    'projected_depletion': 'sum',
                    'is_stockout': 'sum'
                }).reset_index()
                weekly_summary.columns = ['Week', 'Total Projected Depletion', 'Distributors in Stockout']

                st.markdown("**Weekly Aggregate Forecast**")
                st.dataframe(weekly_summary, use_container_width=True, hide_index=True)

                st.markdown("**Full Pipeline Data (for export)**")
                # Show sample of full data
                st.dataframe(pipeline_df.head(50), use_container_width=True, hide_index=True, height=300)

                # Download button for full pipeline data
                csv = pipeline_df.to_csv(index=False)
                st.download_button(
                    label="⬇️ Download Full Pipeline Forecast (CSV)",
                    data=csv,
                    file_name=f"pipeline_forecast_{datetime.now().strftime('%Y%m%d')}.csv",
                    mime="text/csv"
                )
    else:
        st.info("No stockout risk data available")

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
                textfont=dict(color='#2D2926'),
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
                textfont=dict(color='#2D2926'),
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
                    textfont=dict(color='#2D2926')
                )])

                apply_dark_theme(fig, height=400, showlegend=False)
                st.plotly_chart(fig, use_container_width=True)

        else:
            st.info("No product-level data available for selected filters")
    except Exception as e:
        st.warning(f"Could not load product-level data: {e}")

    # US State Map with metric selector
    st.markdown('<p class="section-header">Performance by State</p>', unsafe_allow_html=True)

    try:
        state_df = load_state_depletion_data(lookback_days=lookback_days)

        if not state_df.empty:
            # Calculate avg PODs per door
            state_df['avg_pods'] = state_df['total_pods'] / state_df['total_doors'].replace(0, 1)

            # Metric selector and map in columns
            map_col, selector_col = st.columns([4, 1])

            with selector_col:
                map_metric = st.radio(
                    "Metric",
                    options=['Depletion', 'Doors', 'Total PODs', 'Avg PODs/Door'],
                    index=0,
                    key='map_metric_selector'
                )

            # Map metric to data column and labels
            metric_config = {
                'Depletion': {'col': 'total_depleted', 'label': 'Units', 'format': ',.0f'},
                'Doors': {'col': 'total_doors', 'label': 'Doors', 'format': ',.0f'},
                'Total PODs': {'col': 'total_pods', 'label': 'PODs', 'format': ',.0f'},
                'Avg PODs/Door': {'col': 'avg_pods', 'label': 'Avg PODs', 'format': ',.1f'}
            }

            config = metric_config[map_metric]

            with map_col:
                # Create choropleth map
                fig = go.Figure(data=go.Choropleth(
                    locations=state_df['state'],
                    z=state_df[config['col']],
                    locationmode='USA-states',
                    colorscale=[
                        [0, '#F5F0EB'],
                        [0.25, '#C4D9E8'],
                        [0.5, '#5FA8D3'],
                        [0.75, '#1B4965'],
                        [1, '#0D2B3E']
                    ],
                    colorbar=dict(
                        title=dict(text=config['label'], font=dict(color='#2D2926', size=12)),
                        tickfont=dict(color='#2D2926', size=10),
                        thickness=15,
                        len=0.7
                    ),
                    hovertemplate='<b>%{location}</b><br>' +
                                  f'{config["label"]}: %{{z:{config["format"]}}}<extra></extra>',
                    marker_line_color='rgba(45,41,38,0.15)',
                    marker_line_width=0.5
                ))

                fig.update_layout(
                    geo=dict(
                        scope='usa',
                        bgcolor='rgba(0,0,0,0)',
                        lakecolor='rgba(0,0,0,0)',
                        landcolor='#EDE8E3',
                        showlakes=False,
                        showland=True
                    ),
                    font=dict(family='Jost, Helvetica, sans-serif', color='#2D2926'),
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(0,0,0,0)',
                    margin=dict(l=0, r=0, t=0, b=0),
                    height=350
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
                st.markdown(render_metric_card(f"{total_states}", "States with Depletion"), unsafe_allow_html=True)
            with metric_cols[1]:
                st.markdown(render_metric_card(f"{total_doors:,.0f}", "Total Doors (Active)"), unsafe_allow_html=True)
            with metric_cols[2]:
                st.markdown(render_metric_card(f"{total_pods:,.0f}", "Total PODs"), unsafe_allow_html=True)
            with metric_cols[3]:
                st.markdown(render_metric_card(f"{top_state}", f"Top State ({top_state_pct:.1f}%)"), unsafe_allow_html=True)

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

    # WOI by Distributor x SKU Section
    st.markdown('<p class="section-header">Weeks of Inventory by Distributor x SKU</p>', unsafe_allow_html=True)

    try:
        # Load WOI by SKU data
        # Load WOI data - aggregated at parent distributor level (~50 distributors)
        # Note: filtering by selected distributors would require mapping VIP codes to SF account IDs
        # For now, load all and filter in UI if needed
        woi_sku_df = load_woi_by_sku()

        if not woi_sku_df.empty:
            # Summary metrics for SKU-level (aggregated at parent distributor level)
            woi_metric_cols = st.columns(5)

            with woi_metric_cols[0]:
                parent_distros = woi_sku_df['distributor_name'].nunique()
                st.markdown(render_metric_card(f"{parent_distros}", "Parent Distributors"), unsafe_allow_html=True)
            with woi_metric_cols[1]:
                sku_overstock = len(woi_sku_df[woi_sku_df['inventory_status'] == 'Overstock'])
                st.markdown(render_metric_card(f"{sku_overstock}", "Overstock Combos", "warning"), unsafe_allow_html=True)
            with woi_metric_cols[2]:
                sku_understock = len(woi_sku_df[woi_sku_df['inventory_status'] == 'Understock'])
                st.markdown(render_metric_card(f"{sku_understock}", "Understock Combos", "danger"), unsafe_allow_html=True)
            with woi_metric_cols[3]:
                high_velocity = len(woi_sku_df[woi_sku_df['velocity_tier'] == 'High'])
                st.markdown(render_metric_card(f"{high_velocity}", "High Velocity SKUs"), unsafe_allow_html=True)
            with woi_metric_cols[4]:
                total_combos = len(woi_sku_df)
                st.markdown(render_metric_card(f"{total_combos:,}", "Total SKU x Distro"), unsafe_allow_html=True)

            # Charts: Top Overstock and Understock by SKU × Distributor
            woi_chart_cols = st.columns(2)

            with woi_chart_cols[0]:
                st.markdown("**Top Overstock (SKU × Distributor)**")
                overstock_sku = woi_sku_df[
                    (woi_sku_df['inventory_status'] == 'Overstock') &
                    (woi_sku_df['weeks_of_inventory'].notna())
                ].nlargest(10, 'weeks_of_inventory')

                if not overstock_sku.empty:
                    overstock_sku = overstock_sku.copy()
                    overstock_sku['label'] = overstock_sku['product_name'].str[:25] + ' @ ' + overstock_sku['distributor_name'].str[:20]

                    fig = go.Figure(go.Bar(
                        x=overstock_sku['weeks_of_inventory'],
                        y=overstock_sku['label'],
                        orientation='h',
                        marker=dict(color=COLORS['warning']),
                        text=overstock_sku['weeks_of_inventory'].apply(lambda x: f'{x:.0f} wks'),
                        textposition='outside',
                        textfont=dict(color='#2D2926'),
                        hovertemplate='%{y}<br>WOI: %{x:.1f} weeks<extra></extra>'
                    ))

                    fig.update_layout(
                        paper_bgcolor='rgba(0,0,0,0)',
                        plot_bgcolor='rgba(0,0,0,0)',
                        font=dict(color='#2D2926', family='Jost, Helvetica, sans-serif'),
                        height=350,
                        margin=dict(l=0, r=60, t=10, b=0),
                        yaxis=dict(autorange='reversed', gridcolor='rgba(45, 41, 38, 0.08)'),
                        xaxis=dict(gridcolor='rgba(45, 41, 38, 0.08)')
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No overstock SKU × Distributor combinations")

            with woi_chart_cols[1]:
                st.markdown("**Top Understock (SKU × Distributor)**")
                understock_sku = woi_sku_df[
                    (woi_sku_df['inventory_status'] == 'Understock') &
                    (woi_sku_df['weeks_of_inventory'].notna())
                ].nsmallest(10, 'weeks_of_inventory')

                if not understock_sku.empty:
                    understock_sku = understock_sku.copy()
                    understock_sku['label'] = understock_sku['product_name'].str[:25] + ' @ ' + understock_sku['distributor_name'].str[:20]

                    fig = go.Figure(go.Bar(
                        x=understock_sku['weeks_of_inventory'].abs(),
                        y=understock_sku['label'],
                        orientation='h',
                        marker=dict(color=COLORS['danger']),
                        text=understock_sku['weeks_of_inventory'].apply(lambda x: f'{x:.1f} wks'),
                        textposition='outside',
                        textfont=dict(color='#2D2926'),
                        hovertemplate='%{y}<br>WOI: %{text}<extra></extra>'
                    ))

                    fig.update_layout(
                        paper_bgcolor='rgba(0,0,0,0)',
                        plot_bgcolor='rgba(0,0,0,0)',
                        font=dict(color='#2D2926', family='Jost, Helvetica, sans-serif'),
                        height=350,
                        margin=dict(l=0, r=60, t=10, b=0),
                        yaxis=dict(autorange='reversed', gridcolor='rgba(45, 41, 38, 0.08)'),
                        xaxis=dict(gridcolor='rgba(45, 41, 38, 0.08)')
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No understock SKU × Distributor combinations")

            # WOI Heatmap by Product Category × Distributor (Top 15 by Revenue)
            st.markdown("**Weeks of Inventory by Product (Top 15 Distributors)**")
            st.caption("🟢 Green = Balanced (2-8 wks) | 🔴 Red = Understock (<2 wks) | 🟡 Yellow = Overstock (>8 wks)")

            # Filter data for meaningful WOI display:
            # - Exclude 25mg products (discontinued)
            # - Exclude null WOI (non-VIP distributors with no depletion data)
            # - Exclude extreme outliers (WOI > 52 weeks = 1 year, likely data issues)
            heatmap_source = woi_sku_df[
                (~woi_sku_df['product_category'].str.contains('25mg', na=False)) &
                (woi_sku_df['weeks_of_inventory'].notna()) &
                (woi_sku_df['weeks_of_inventory'].between(-52, 52))
            ]

            heatmap_df = heatmap_source.groupby(['distributor_name', 'product_category']).agg({
                'weeks_of_inventory': 'mean',
                'qty_depleted': 'sum',
                'order_value': 'sum'
            }).reset_index()

            if len(heatmap_df) > 0:
                pivot_df = heatmap_df.pivot(
                    index='distributor_name',
                    columns='product_category',
                    values='weeks_of_inventory'
                ).fillna(0)

                # Select top 15 distributors by revenue (order_value), sorted descending
                distro_revenue = heatmap_df.groupby('distributor_name')['order_value'].sum().nlargest(15)
                top_distros = distro_revenue.index.tolist()
                pivot_df = pivot_df.reindex(top_distros)

                # Reorder columns by product family (Bottles, Seltzers, Shots)
                column_order = [
                    '2mg 750ml Bottle', '5mg 750ml Bottle', '10mg 750ml Bottle',
                    '10mg 16oz Seltzer', '5mg 12oz Seltzer',
                    '5mg 2oz Shot', '10mg 2oz Shot'
                ]
                ordered_cols = [c for c in column_order if c in pivot_df.columns]
                pivot_df = pivot_df[ordered_cols]

                # Shorten column names for readability
                short_names = {
                    '2mg 750ml Bottle': '2mg Bottle',
                    '5mg 750ml Bottle': '5mg Bottle',
                    '10mg 750ml Bottle': '10mg Bottle',
                    '10mg 16oz Seltzer': '10mg Seltzer',
                    '5mg 12oz Seltzer': '5mg Seltzer',
                    '5mg 2oz Shot': '5mg Shot',
                    '10mg 2oz Shot': '10mg Shot'
                }
                pivot_df.columns = [short_names.get(c, c) for c in pivot_df.columns]

                # Clip extreme values for better color scaling (-52 to +52 weeks)
                z_values = pivot_df.values.clip(-52, 52)

                if not pivot_df.empty:
                    fig = go.Figure(data=go.Heatmap(
                        z=z_values,
                        x=pivot_df.columns.tolist(),
                        y=pivot_df.index.tolist(),
                        colorscale=[
                            [0, '#b04d5e'],      # Muted red = understock
                            [0.25, '#D4917A'],   # Warm salmon
                            [0.4, '#85C79D'],    # Sage green = balanced
                            [0.6, '#85C79D'],    # Sage green
                            [0.75, '#C4A77D'],   # Warm gold = overstock
                            [1, '#8a6b00']       # Deep gold = high overstock
                        ],
                        zmin=-52,
                        zmax=52,
                        zmid=4,
                        text=pivot_df.values.round(1),
                        texttemplate='%{text}',
                        textfont={"size": 13, "color": "#2D2926", "family": "Jost, Helvetica, sans-serif"},
                        hovertemplate='<b>%{y}</b><br>%{x}<br>WOI: %{z:.1f} weeks<extra></extra>'
                    ))

                    fig.update_layout(
                        paper_bgcolor='rgba(0,0,0,0)',
                        plot_bgcolor='rgba(0,0,0,0)',
                        font=dict(color='#2D2926', size=12),
                        height=600,
                        margin=dict(l=250, r=50, t=80, b=30),
                        xaxis=dict(
                            tickangle=0,
                            tickfont=dict(size=12, color='#2D2926'),
                            side='top'
                        ),
                        yaxis=dict(
                            tickfont=dict(size=11, color='#2D2926'),
                            automargin=True
                        ),
                        coloraxis_colorbar=dict(
                            title="WOI (weeks)",
                            tickvals=[-52, -26, 0, 4, 13, 26, 52],
                            ticktext=['-52', '-26', '0', '4', '13', '26', '52']
                        )
                    )
                    st.plotly_chart(fig, use_container_width=True)

            # Detailed SKU × Distributor Table
            st.markdown("**WOI Details by SKU × Distributor**")

            woi_filter_cols = st.columns(3)
            with woi_filter_cols[0]:
                woi_distro_filter = st.multiselect(
                    "Filter by Distributor",
                    options=['All'] + sorted(woi_sku_df['distributor_name'].dropna().unique().tolist()),
                    default=['All'],
                    key='woi_distro_filter'
                )
            with woi_filter_cols[1]:
                woi_status_filter = st.multiselect(
                    "Filter by Status",
                    options=['All', 'Overstock', 'Understock', 'Balanced', 'No Depletion', 'No Orders'],
                    default=['All'],
                    key='woi_status_filter'
                )
            with woi_filter_cols[2]:
                woi_category_filter = st.multiselect(
                    "Filter by Product Category",
                    options=['All'] + sorted(woi_sku_df['product_category'].dropna().unique().tolist()),
                    default=['All'],
                    key='woi_category_filter'
                )

            display_woi = woi_sku_df.copy()
            if 'All' not in woi_distro_filter:
                display_woi = display_woi[display_woi['distributor_name'].isin(woi_distro_filter)]
            if 'All' not in woi_status_filter:
                display_woi = display_woi[display_woi['inventory_status'].isin(woi_status_filter)]
            if 'All' not in woi_category_filter:
                display_woi = display_woi[display_woi['product_category'].isin(woi_category_filter)]

            # Add product family sort order (group by format: Bottles, Seltzers, Shots)
            category_sort_order = {
                # 750ml Bottles
                '2mg 750ml Bottle': 10, '5mg 750ml Bottle': 11, '10mg 750ml Bottle': 12,
                # Seltzers (16oz and 12oz together)
                '10mg 16oz Seltzer': 20, '5mg 12oz Seltzer': 21,
                # 2oz Shots
                '5mg 2oz Shot': 30, '10mg 2oz Shot': 31
            }
            display_woi['_sort_order'] = display_woi['product_category'].map(category_sort_order).fillna(99)

            # Include dc_count to show how many DCs rolled up under each parent
            display_cols = display_woi[[
                'distributor_name', 'product_name', 'product_category',
                'qty_ordered', 'qty_depleted', 'weekly_depletion_rate',
                'weeks_of_inventory', 'inventory_status', 'velocity_tier', 'dc_count', '_sort_order'
            ]].copy()

            display_cols['weeks_of_inventory'] = display_cols['weeks_of_inventory'].apply(
                lambda x: f"{x:.1f}" if pd.notna(x) else "N/A"
            )

            # Sort by distributor, then product family
            display_cols = display_cols.sort_values(['distributor_name', '_sort_order', 'product_name'])
            display_cols = display_cols.drop(columns=['_sort_order'])

            display_cols.columns = [
                'Distributor', 'Product', 'Category', 'Qty Ordered', 'Qty Depleted',
                'Weekly Rate', 'WOI', 'Status', 'Velocity', 'DCs'
            ]

            st.dataframe(
                display_cols,
                use_container_width=True,
                hide_index=True,
                height=500
            )

        else:
            st.info("No WOI by SKU data available")

    except Exception as e:
        st.warning(f"Could not load WOI by SKU data: {e}")

    # Footer
    st.markdown(f"""
    <div style="text-align: center; color: #625f56; margin-top: 48px; padding: 24px; border-top: 1px solid rgba(45,41,38,0.1);">
        <p style="margin: 0;">Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} UTC</p>
        <p style="margin: 4px 0 0 0; font-size: 12px;">Data refreshes every 5 minutes | Lookback: {lookback_days} days</p>
    </div>
    """, unsafe_allow_html=True)


if __name__ == "__main__":
    main()
