# Task 3 — Enterprise Data Platform

**Objective:** Transition NovaFinds from ad-hoc local scripts to a scalable, governed data platform that can support both real-time business operations and advanced data science.

---

## The Starting Point

When this engagement begins, NovaFinds' data infrastructure looks like this:

- A single PostgreSQL database with no orchestration
- Local Python scripts run manually on individual machines
- Every department builds its own pipelines from the same source tables
- No data quality checks, no lineage, no alerting
- Dashboards go stale silently when pipelines fail
- 27.34% of customers have conflicting status records — no single source of truth
- Finance and Marketing are almost certainly reporting different revenue numbers

This is not a technical failure — it is what reasonable startup infrastructure looks like. The problem is that 300% transaction growth and international expansion have made it fragile.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                          UNITY CATALOG                              │
│                Governance · Lineage · Access Control                │
├──────────────┬─────────────────────────────────────┬───────────────┤
│   SOURCES    │           MEDALLION LAYERS           │   SERVING     │
├──────────────┼──────────┬──────────┬───────────────┼───────────────┤
│              │          │          │               │               │
│ PostgreSQL   │  BRONZE  │  SILVER  │     GOLD      │ BI Dashboards │
│ (Lakeflow    │──────────│──────────│───────────────│ Self-serve SQL│
│  Connect)  ──►  Raw     │  Cleaned │  Business     │ Data Science  │
│              │  landing │  joined  │  metrics      │ Marketing     │
│ Stripe       │  10 tbls │  4 tbls  │  7 tbls       │               │
│ (Fivetran) ──►  Immut.  │  DQ      │  KPIs         │               │
│              │  history │  checks  │  USD norm.    │               │
└──────────────┴──────────┴──────────┴───────────────┴───────────────┘

Compute: Jobs Compute with Photon  │  Orchestration: DLT Triggered mode
```

### Ingestion — Managed Connectors

Rather than writing and maintaining custom ingestion code, two managed connectors handle source data:

**Lakeflow Connect** — connects to PostgreSQL with Change Data Capture (CDC). Detects inserts, updates, and deletes at the row level and propagates them incrementally. No full reloads, no manual scheduling.

**Fivetran (Stripe connector)** — syncs Stripe payment data automatically. At approximately $120/month for the Starter tier at current volume (~1k payments/month), significantly cheaper than the engineering time required to maintain a custom Stripe integration.

### Transformation — Lakeflow Pipelines (Delta Live Tables)

Transformations follow the medallion architecture across three layers:

**Bronze** — raw data as it arrives from connectors. No transformations, no filters. Immutable historical record — if something goes wrong downstream, you can always reprocess from here.

**Silver** — cleaned, joined, and validated data. This is where data quality expectations are enforced:

| Entity | Hard blocks (FAIL UPDATE) | Soft blocks (DROP ROW) |
|---|---|---|
| `payment_silver` | `payment_id IS NOT NULL`, `amount >= 0` | — |
| `product_silver` | `product_id IS NOT NULL`, `price > 0` | — |
| `order_silver` | `order_id IS NOT NULL`, `quantity > 0` | — |
| `customer_silver` | — | `country IS NULL`, `active IS NULL` |

Hard blocks stop the pipeline — the data is unusable and proceeding would corrupt downstream metrics. Soft blocks drop the offending row and log it — the pipeline continues but the anomaly is recorded and visible in the monitoring dashboard.

Silver also applies multi-currency normalisation across 50+ currencies, converting `total_amount` to `unified_amount` in USD using static FX rates. This is the step that makes cross-regional revenue figures comparable.

**Gold** — 7 pre-aggregated tables ready for dashboards and self-serve SQL:

| Table | Purpose |
|---|---|
| `product_revenue_gold` | Revenue and cost per SKU |
| `cancelled_product_gold` | Return rates by product |
| `sales_region_gold` | Revenue by APAC / EMEA / LATAM / North America |
| `sales_country_gold` | Country-level drill-down |
| `stripe_payment_gold` | Payment method performance and success rates |
| `currency_analysis_gold` | Revenue by currency and FX impact |
| `payment_source_comparison_gold` | PostgreSQL vs Stripe revenue reconciliation |

### Compute — Jobs Compute with Photon

The pipeline runs on **Jobs Compute with Photon** rather than Serverless. At current volume (~1k payments/month) in Triggered mode, this costs approximately $15–30/month for compute vs higher Serverless rates for equivalent work. Photon provides vectorised execution that benefits the SQL-heavy Silver and Gold transformations. Switch to Serverless if cold-start latency becomes a constraint or pipeline frequency increases significantly.

**Estimated total monthly cost: $145–170**
- Lakeflow Connect (PostgreSQL CDC): $5–10
- Jobs Compute with Photon: $15–30
- Storage (Unity Catalog): $5–10
- Fivetran Starter (Stripe): $120

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

## How Task 1 Evolves in This Environment

The `simple_stripe_loader.py` from Task 1 and the Lakeflow Pipeline in this task solve the same problem at different levels of maturity:

| Dimension | Task 1 | Task 3 |
|---|---|---|
| **Ingestion** | Local JSON file | Fivetran Starter connector, automatic sync |
| **Field mapping** | Explicit Python | Declarative SQL in Silver layer |
| **Schema evolution** | `ALTER TABLE ADD COLUMN` per run | Unity Catalog governed, automatic |
| **Idempotency** | Application-level check + UNIQUE constraint | DLT exactly-once semantics via checkpointing |
| **Data quality** | None | 12+ EXPECT constraints with hard/soft blocks |
| **Logging** | `print()` statements | Observable pipeline event log |
| **Alerting** | None | Slack, Email, PagerDuty |
| **Orchestration** | Run manually | Scheduled, automatically retried |
| **Lineage** | None | Unity Catalog column-level lineage |
| **Monthly cost** | Free (local) | ~$145–170/month |

The business logic is the same. The infrastructure is production-grade.

---

## How to Deploy

**Prerequisites:** A Databricks workspace with Unity Catalog enabled, a Lakeflow Connect PostgreSQL connection, and a Fivetran Stripe connector configured.

```
1. Upload NovaFinds_Lakeflow_Pipeline.py to your Databricks workspace

2. Create the DLT pipeline:
   Workflows → Delta Live Tables → Create Pipeline
   - Pipeline name: NovaFinds E-Commerce Pipeline
   - Product edition: Advanced
   - Notebook: select this file
   - Target: main.novafinds
   - Cluster mode: Jobs Compute → enable Photon acceleration
   - Pipeline mode: Triggered

3. Update source paths in Bronze layer cells to match your
   Lakeflow Connect and Fivetran catalog/schema names

4. Start the pipeline and verify data quality metrics in the UI

5. Schedule the monitoring notebook:
   - Frequency: hourly, 10 minutes after pipeline runs
   - Update pipeline_id in Cell 2 with your pipeline ID
```

**Finding your pipeline ID:** Open your DLT pipeline in the Databricks UI and copy the ID from the URL: `.../delta-live-tables/pipelines/YOUR-ID-HERE`
