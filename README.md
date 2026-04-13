# NovaFinds Data Platform

**ADC Consulting — Data Engineering Case Study**

---

## The Business Context

NovaFinds is a mid-sized, rapidly scaling e-commerce retailer specialising in outdoor adventure gear. Over the last 12 months the company has seen 300% transaction volume growth and is expanding into international markets — particularly APAC, where it already generates 43% of attributed revenue despite no deliberate regional strategy.

The problem is that growth has outpaced the company's ability to understand itself. Two live data sources — a PostgreSQL database (customers, orders, products) and Stripe (payments, already live) — operate in silos. Neither alone can answer the questions that matter: which products are actually profitable, which customer segments drive retention, and where to invest next.

This case study designs and implements the data platform that changes that.

---

## What the Data Actually Shows

Before jumping to architecture, the analysis of the existing data reveals several important findings:

**What we can confirm:**
- APAC drives 43% of total attributed revenue — North America barely registers ($131k vs $4k)
- 57% of customers order 4–5+ times per year — loyalty is a genuine asset
- Four products lead on total revenue: Pet Travel Carrier, Multi-Port USB Hub, Protein Pancake Mix, Frozen Salmon Filets
- NovaFinds operates across 20+ product categories — a wide spread with no dominant segment

**Data quality failures blocking deeper analysis:**
- No sell price per order line item — margin analysis is impossible across all 500+ SKUs
- 16% of orders have no region attribution — $53k of revenue is unassigned
- 25.78% of customers are missing country registration
- 27.34% of customers have a conflicting active/inactive status — there is no single source of truth for the customer base
- Payment data gaps are being resolved through Stripe onboarding (Task 1)

These aren't minor gaps. They mean NovaFinds cannot reliably segment its customers, measure campaign ROI, or trust the revenue figures it currently reports.

---

## Strategic Priorities

Three priorities emerge directly from the data:

**1. Double down on APAC** — the market is already there, but NovaFinds is flying blind. Understanding which products drive APAC sales, by sub-region, is the next step.

**2. Activate star products for customer acquisition** — the loyal customer base (57% repeat buyers) is proof of product-market fit. The four revenue-leading SKUs should anchor paid social and marketing campaigns to bring in new buyers.

**3. Build supply chain resilience** — 20+ product categories across global markets at 300% growth creates a highly complex supplier network. Without a unified data model, this becomes an operational risk.

All three require the same foundation: integrated, trusted data from PostgreSQL and Stripe.

---

## Repository Structure

```
novafinds-data-platform/
│
├── README.md                              ← You are here
│
├── task1-stripe-integration/
│   ├── README.md                          ← Design logic and how to run simple_stripe_loader.py
│   └── simple_stripe_loader.py            ← Stripe → PostgreSQL pipeline
│
├── task2-gold-layer/
│   ├── README.md                                   ← analysis using gold layer logic, answer to the questions, and how to run the .py or .ipynb below
│   ├── task2_postgresql_pipeline.py                ← the runnable local version on Docker
│   └── task2_databricks_business_analytics.ipynb   ← Kimball Gold layer notebook originally ran on Databricks, includes some analysis results (charts and graphs)
│
└── task3-data-platform/
    ├── README.md                          ← Architecture & migration rationale
    ├── NovaFinds_Lakeflow_Pipeline.py     ← Bronze → Silver → Gold DLT pipeline
    ├── NovaFinds_Pipeline_Monitoring.py   ← Monitoring & alerting notebook
    └── novafinds-pipeline-dabs
       ├── databricks.yml                     ← DAB bundle config
       ├── resources/
       │   ├── pipeline.yml                   ← Lakeflow DLT definition
       │   └── jobs.yml                       ← CI/CD job orchestration
       ├── src/novafinds/
       │   ├── pipeline_notebook.py           ← Medallion pipeline logic
       │   └── visualization_dashboard.py
       └── tests/unit/
          └── test_runner.py
```

## Task Overview

| Task | What it does | Key technology |
|---|---|---|
| **Task 1** | Ingest Stripe into the existing `payment` table without breaking dashboards | Python, psycopg2, JSONB schema evolution |
| **Task 2** | Gold layer analytical model for product, cancellation, and regional analysis | Kimball dimensional modelling, SQL |
| **Task 3** | Enterprise platform with CI/CD deployment across dev / stage / prod | Databricks Asset Bundle, DLT, Lakeflow, Unity Catalog |

---

## Local Setup (Task 1 + 2)

**Prerequisites:** Docker, Python 3.8+

```bash
# Start the database
docker compose up -d

pip install psycopg2-binary pandas

# Task 1 — ingest Stripe payments
cd task1-stripe-integration
python simple_stripe_loader.py

# Task 2 — build Silver + Gold layers
cd ../task2-gold-layer
python task2_postgresql_pipeline.py
```

## Platform Setup (Task 3)

**Prerequisites:** Databricks CLI, workspace access

```bash
cd task3-data-platform
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run novafinds_integration_pipeline -t dev
```

See `task3-data-platform/QUICKSTART.md` for full setup, and `CICD.md` for GitHub Actions configuration.

---

## The Thread Connecting All Three Tasks

Task 1 solves an immediate problem — connect Stripe to PostgreSQL without disrupting reporting. Task 2 builds the analytical layer that makes the data useful. Task 3 shows what both of those look like inside a production-grade, version-controlled, CI/CD-deployed platform.

The `simple_stripe_loader.py` from Task 1 becomes a Fivetran-managed source in Task 3. The Gold tables from Task 2 become DLT-declared, quality-checked, lineage-tracked outputs in Task 3. The same logic — at a different level of engineering maturity.
