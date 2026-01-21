# SaaS Analytics End-to-End Data Pipeline 🚀

## Overview
This project is an end-to-end **SaaS Analytics Data Pipeline** built using a **Medallion Architecture (Bronze → Silver → Gold)**.  
It ingests raw SaaS data, performs transformations and data quality checks, produces BI-ready aggregates, and delivers insights via dashboards and EDA.

The pipeline is fully automated and scheduled for daily execution.

---

## Architecture
Raw CSVs --> Bronze layer(Raw Ingestion) --> Silver Layer(Cleaning, Validation, Enrichment) --> Gold Layer(Business Aggregates) --> EDA + Dashboard


---

## Tech Stack
- **Database**: PostgreSQL
- **Language**: Python
- **Libraries**: pandas, SQLAlchemy, matplotlib
- **BI / Visualization**: Streamlit
- **Scheduling**: CRON
- **Version Control**: Git & GitHub

---

## Data Model

### Bronze (Raw)
- customers
- users
- subscriptions
- payments
- usage_events

### Silver (Processed)
- silver.customers
- silver.users
- silver.subscriptions
- silver.payments
- silver.usage_events

Includes:
- Type casting
- Date parsing
- Derived columns
- Foreign key validation
- Rejected rows logged to `audit.rejected_rows`

### Gold (Analytics)
- gold.mrr_monthly
- gold.active_customers_monthly
- gold.customer_churn_monthly
- gold.dau_mau_monthly
- gold.dashboard_monthly

---

## Key Metrics
- Monthly Recurring Revenue (MRR)
- Active Customers
- Customer Churn Rate
- DAU / MAU Ratio
- Subscription Lifecycle Trends

---

## Data Quality Framework
- Primary key uniqueness checks
- Foreign key validation
- Range checks (amounts, counts)
- Rejected rows captured in audit schema with rule metadata

---

## EDA
Exploratory Data Analysis is available under `/eda/`:
- Revenue trends
- Churn behavior
- Customer growth patterns
- User engagement analysis

---

## Automation
The entire pipeline is automated using **CRON**:

```bash
python3 -m src.etl all

# Install dependencies
pip install -r requirements.txt

# Run full pipeline
python3 -m src.etl all

# Launch dashboard
streamlit run dashboard.py

Author: Karthik K
