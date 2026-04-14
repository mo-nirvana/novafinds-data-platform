# Task 3 — Enterprise Data Platform

**Objective:** Transition NovaFinds from ad-hoc local scripts to a scalable, governed data platform — deployable via CI/CD using a Databricks Asset Bundle.

---

## Repository Structure

```
novafinds-pipeline-dabs/
├── databricks.yml                        ← Bundle config: dev / stage / prod targets
├── resources/
│   ├── pipeline.yml                      ← Lakeflow DLT pipeline definition
│   └── jobs.yml                          ← Orchestration: tests → pipeline → viz
├── src/novafinds/
│   ├── pipeline_notebook.py              ← Bronze → Silver → Gold medallion logic
│   └── visualization_dashboard.py        ← Post-pipeline analytics output
└── tests/unit/
    └── test_runner.py                    ← Unit tests run before every deployment
```

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                          UNITY CATALOG                              │
│                Governance · Lineage · Access Control                │
├──────────────┬─────────────────────────────────────┬───────────────┤
│   SOURCES    │           MEDALLION LAYERS           │   SERVING     │
├──────────────┼──────────┬──────────┬───────────────┼───────────────┤
│ PostgreSQL   │  BRONZE  │  SILVER  │     GOLD      │ BI Dashboards │
│ (Lakeflow    ├──────────┤──────────┤───────────────┤ Self-serve SQL│
│  Connect)  ──► Raw      │ Cleaned  │ Business      │ Data Science  │
│              │ landing  │ joined   │ metrics       │ Marketing     │
│ Stripe       │ 10 tbls  │ 4 tbls   │ 7 tbls        │               │
│ (Fivetran) ──► Immut.   │ DQ       │ KPIs +        │               │
│              │ history  │ checks   │ USD norm.     │               │
└──────────────┴──────────┴──────────┴───────────────┴───────────────┘
Compute: Jobs Compute with Photon (prod)  │  Orchestration: DLT Triggered
```

### Why Databricks Asset Bundle (DAB)?

The original approach was a standalone Databricks notebook deployed manually. Replacing it with a DAB brings the pipeline into a proper software development lifecycle:

| Without DAB | With DAB |
|---|---|
| Manual notebook deployment | `databricks bundle deploy -t prod` |
| No environment separation | Isolated dev / stage / prod catalogs |
| No automated tests before deploy | Unit tests gate every deployment |
| Config changes require UI edits | Everything version-controlled in YAML |
| No CI/CD integration | GitHub Actions: PR → dev, merge → stage, tag → prod |

---

## CI/CD Workflow

```
Pull Request  →  validate + deploy dev  →  run unit tests
Merge to main →  deploy staging         →  run integration tests
Release tag   →  deploy production      →  scheduled daily at 02:00 UTC
```

Each environment uses an isolated Unity Catalog:
- `novafinds_dev` — small fixture dataset, rapid iteration
- `novafinds_stage` — full dataset, pre-production validation
- `novafinds_prod` — live data, Photon enabled, email alerts on failure

### Job Orchestration

Each deployment runs a three-task job in sequence:

```
unit_tests → lakeflow_pipeline → data_visualization
```

If unit tests fail, the pipeline does not run. If the pipeline fails, visualisation does not run. Failures at any stage trigger email notification.

---

## Data Quality Framework

| Entity | Hard blocks (FAIL UPDATE) | Soft blocks (DROP ROW) |
|---|---|---|
| `payment_silver` | `payment_id IS NOT NULL`, `amount >= 0` | — |
| `product_silver` | `product_id IS NOT NULL`, `price > 0 & sell_price > 0` | — |
| `order_silver` | `order_id IS NOT NULL`, `quantity > 0` | — |
| `customer_silver` | — | `country IS NULL`, `active IS NULL` |

Hard blocks stop the pipeline loudly. Soft blocks drop the row and log it — visible in the DLT pipeline UI and the monitoring notebook.

---

## Gold Layer Tables

| Table | Purpose |
|---|---|
| `product_revenue_gold` | Revenue and cost per SKU |
| `cancelled_product_gold` | Return rates by product |
| `sales_region_gold` | Revenue by APAC / EMEA / LATAM / North America |
| `sales_country_gold` | Country-level drill-down |
| `stripe_payment_gold` | Payment method performance and success rates |
| `currency_analysis_gold` | Revenue by currency and FX impact |
| `payment_source_comparison_gold` | PostgreSQL vs Stripe revenue reconciliation |

---

## Quickstart

**Prerequisites:** Databricks CLI installed, workspace URL and token configured.

```bash
# 1. Authenticate
databricks configure

# 2. Validate
databricks bundle validate -t dev

# 3. Deploy to dev
databricks bundle deploy -t dev

# 4. Run full pipeline (tests → pipeline → visualisation)
databricks bundle run novafinds_integration_pipeline -t dev

# 5. Promote to staging
databricks bundle deploy -t stage
databricks bundle run novafinds_integration_pipeline -t stage

# 6. Deploy to production (runs on schedule after this)
databricks bundle deploy -t prod
```

See `QUICKSTART.md` for full setup including catalog creation and sample data upload. See `CICD.md` for GitHub Actions configuration.

---

## Estimated Monthly Cost (Production)

| Component | Cost |
|---|---|
| Lakeflow Connect (PostgreSQL CDC) | $5–10 |
| Jobs Compute with Photon | $15–30 |
| Storage (Unity Catalog) | $5–10 |
| Fivetran Starter (Stripe) | $120 |
| **Total** | **$145–170/month** |

---

## How Task 1 Evolves Here

| Dimension | Task 1 | Task 3 |
|---|---|---|
| Deployment | Run manually | `databricks bundle deploy` |
| Environments | Single local DB | dev / stage / prod isolation |
| Testing | None | Unit tests gate every deploy |
| Stripe ingestion | Local JSON file | Fivetran, automatic sync |
| Data quality | None | 12+ DLT expectations |
| Alerting | `print()` | Email on failure, DLT event log |
| Version control | Script file | Full bundle in Git |



### Governance — Unity Catalog

Unity Catalog sits above the entire stack and provides:

- **Lineage** — every table knows where its data came from and what depends on it
- **Access control** — fine-grained permissions per table, column, and row
- **Data discovery** — a searchable catalogue of all tables with owners, descriptions, and quality metrics
- **A single definition of "Revenue"** — when Finance and Marketing both query `sales_region_gold`, they get the same number, computed the same way, from the same source

### Monitoring — Pipeline Monitoring Notebook

The `NovaFinds_Pipeline_Monitoring.py` notebook runs hourly (10 minutes after the pipeline completes) and checks:

1. **Pipeline run history** — status, duration, trigger type for the last 24 hours
2. **Data quality expectations** — pass/fail rates for all Silver layer constraints
3. **Row count deltas** — rows dropped between Bronze and Silver (soft block violations)
4. **Anomaly detection** — flags if pipeline duration spikes more than 2 standard deviations above the 7-day average
5. **Connector health** — CDC lag for Lakeflow Connect, sync status for Fivetran

**Alert thresholds:**

| Metric | Warning | Critical | Channel |
|---|---|---|---|
| Pipeline duration | > 30 min | > 60 min | Email |
| Expectation pass rate | < 95% | < 90% | Slack + Email |
| Row drop rate | > 10% | > 30% | Slack + Email |
| CDC lag | > 10 min | > 30 min | Email |
| Failed runs in 24h | 1 | 3+ | PagerDuty |

---

## Migration Path

**Phase 1 (1 week): Stripe–PostgreSQL integration**
- `simple_stripe_loader.py` (Task 1) goes to production
- 16% null-region resolved via Stripe billing address join
- Legacy dashboards remain unbroken throughout

**Phase 2 (1–4 weeks): Lakeflow medallion platform**
- Local Python scripts migrated to Lakeflow Pipelines
- Bronze → Silver → Gold layers established in Databricks
- Cron jobs replaced with orchestrated, observable workflows
- Silver layer data quality expectations and SLA agreements added

**Phase 3 (2 weeks): Governance and self-serve analytics**
- Unity Catalog deployed — lineage, access control, data discovery
- Single semantic layer publishes certified definitions of Revenue, Active Customer, Gross Margin
- Gold layer serving BI tools and Data Science
- APAC growth dashboard live for the Marketing team

Each phase delivers standalone value before the next begins. Realistic for a team of 1–2 FTE.

---
