# Ops Command Center

Unified multi-page Streamlit dashboard consolidating all operations analytics:

- **Data Quality** - VIP ↔ Salesforce alignment, match rates, duplicate detection
- **Distributor Inventory** - Salesforce orders vs VIP depletion, overstock/understock analysis
- **ShipStation Fulfillment** - B2B order fulfillment, carrier performance, shipping metrics
- **Zendesk Support** - B2C customer support analytics, CSAT, agent performance

## Quick Start

### Run Locally

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # or `venv\Scripts\activate` on Windows

# Install dependencies
pip install -r requirements.txt

# Configure secrets (copy example and fill in values)
cp .streamlit/secrets.toml.example .streamlit/secrets.toml
# Edit secrets.toml with your credentials

# Run dashboard
streamlit run app.py
```

### Deploy to Streamlit Cloud

1. Push code to GitHub
2. Go to [share.streamlit.io](https://share.streamlit.io)
3. Connect GitHub repo
4. Add secrets in Streamlit Cloud settings:
   - Copy contents of `secrets.toml.example`
   - Replace with your actual credentials
5. Auto-deploys on push to main branch

## Project Structure

```
ops-command-center/
├── app.py                      # Main home page with dashboard cards
├── pages/
│   ├── 1_Data_Quality.py       # VIP ↔ Salesforce alignment
│   ├── 2_Distributor_Inventory.py  # Orders vs depletion analysis
│   ├── 3_ShipStation_Fulfillment.py  # B2B fulfillment metrics
│   └── 4_Zendesk_Support.py    # B2C support analytics
├── .streamlit/
│   ├── config.toml             # Theme and server config
│   └── secrets.toml.example    # GCP credentials template
├── requirements.txt
└── README.md
```

## Data Sources

### BigQuery Datasets

| Dataset | Purpose |
|---------|---------|
| `staging_data_quality` | VIP/SF alignment views |
| `staging_vip.distributor_fact_sheet_v2` | Distributor master |
| `staging_salesforce.salesforce_orders_flattened` | SF orders |
| `raw_vip.sales_lite` | VIP depletion transactions |
| `mart_shipstation` | ShipStation fact/dim tables |
| `mart_zendesk` | Zendesk fact/dim tables |

## Authentication

The dashboard uses Google sign-in via Streamlit Cloud:
1. After deploy, go to app settings → Sharing
2. Add team email addresses
3. Viewers must sign in with Google to access

## Cost

- **BigQuery queries**: ~$0.40/mo (5-min cached queries across all pages)
- **Streamlit Cloud**: FREE (Community Cloud with one private app)

---

Built with Streamlit, consolidating 4 dashboards into one multi-page app.
