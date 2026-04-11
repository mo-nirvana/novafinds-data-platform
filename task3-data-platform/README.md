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
- 27.34% of customers have conflicting status records — there is no agreed definition of who is an "active customer"
- Finance and Marketing are almost certainly reporting different revenue numbers

This is not a technical failure — it is what reasonable startup infrastructure looks like. The problem is that 300% transaction growth and international expansion have made it fragile.

---

## Architecture

The platform is built on Databricks with three layers of concern:

### Ingestion — Managed Connectors

Rather than writing and maintaining custom ingestion code, two managed connectors handle source data:

**Lakeflow Connect** — connects to PostgreSQL with Change Data Capture (CDC). It detects inserts, updates, and deletes at the row level and propagates them incrementally. No full reloads, no manual scheduling.

**Fivetran (Stripe connector)** — syncs Stripe payment data automatically. At approximately $135–150/month for the Starter tier at current volume (~1k payments/month), this is significantly cheaper than the engineering time required to maintain a custom Stripe integration.

The alternative — writing and maintaining two custom connectors — would consume the majority of a 0.5–2 FTE team's capacity, leaving no time to build the analytical layer.

### Transformation — Lakeflow Pipelines (Delta Live Tables)

Transformations follow the medallion architecture across three layers:

**Bronze** — raw data as it arrives from the connectors. No transformations, no filters. The Bronze layer is an immutable historical record — if something goes wrong downstream, you can always reprocess from here.

**Silver** — cleaned, joined, and validated data. This is where data quality expectations are enforced:

| Table | Hard blocks (FAIL UPDATE) | Soft blocks (DROP ROW) |
|---|---|---|
| `payment_silver` | `payment_id IS NOT NULL`, `amount >= 0` | — |
| `customer_silver` | — | `country IS NULL`, `active IS NULL` |
| `product_silver` | `product_id IS NOT NULL`, `price > 0` | — |
| `order_silver` | `order_id IS NOT NULL`, `quantity > 0` | — |

Hard blocks stop the pipeline — the data is unusable and proceeding would corrupt downstream metrics. Soft blocks drop the offending row and log it — the pipeline continues, but the anomaly is recorded and visible in the monitoring dashboard.

**Gold** — pre-aggregated business metrics ready for dashboards and self-serve SQL. Four tables serve the primary analytical use cases (product profitability, cancellation rates, regional sales, country sales). See Task 2 for the full dimensional model design.

### Governance — Unity Catalog

Unity Catalog sits above the entire stack and provides:

- **Lineage** — every table knows where its data came from and what depends on it
- **Access control** — fine-grained permissions per table, column, and row
- **Data discovery** — a searchable catalogue of all tables with owners, descriptions, and quality metrics
- **A single definition of "Revenue"** — when Finance and Marketing both query `sales_region_gold`, they get the same number, computed the same way, from the same source

This is the governance layer that prevents the current situation — where different teams query different tables and get different answers — from recurring.

### Monitoring — Pipeline Monitoring Notebook

The `NovaFinds_Pipeline_Monitoring.py` notebook runs hourly (10 minutes after the pipeline completes) and checks:

1. **Pipeline run history** — status, duration, trigger type for the last 24 hours
2. **Data quality expectations** — pass/fail rates for all Silver layer constraints
3. **Row count deltas** — how many rows were dropped between Bronze and Silver (soft blocks)
4. **Anomaly detection** — flags if pipeline duration spikes more than 2 standard deviations above the 7-day average
5. **Connector health** — CDC lag for Lakeflow Connect, sync status for Fivetran

**Alert thresholds:**

| Metric | Warning | Critical | Alert channel |
|---|---|---|---|
| Pipeline duration | > 30 min | > 60 min | Email |
| Expectation pass rate | < 95% | < 90% | Slack + Email |
| Row drop rate | > 10% | > 30% | Slack + Email |
| CDC lag | > 10 min | > 30 min | Email |
| Failed runs in 24h | 1 | 3+ | PagerDuty |

---

## Migration Path

The transition from local scripts to the enterprise platform is designed to be phased — each phase delivers standalone value before the next begins.

**Phase 1 (0–30 days): Stripe–PostgreSQL integration**
- `simple_stripe_loader.py` (Task 1) goes to production
- 16% null-region resolved via Stripe billing address join
- Legacy dashboards remain unbroken throughout

**Phase 2 (30–90 days): Lakeflow medallion platform**
- Local Python scripts migrated to Lakeflow Pipelines
- Bronze → Silver → Gold layers established in Databricks
- Cron jobs replaced with orchestrated, observable workflows
- Silver layer data quality expectations added

**Phase 3 (90–180 days): Governance and self-serve analytics**
- Unity Catalog deployed — lineage, access control, data discovery
- Single semantic layer publishes certified definitions of Revenue, Active Customer, Gross Margin
- Gold layer serving BI tools and Data Science
- APAC growth dashboard live for the Marketing team

This phasing is realistic for a team of 0.5–2 FTE. Each phase has a clear deliverable that justifies the next investment.

---

## How the Task 1 Script Evolves

The `simple_stripe_loader.py` from Task 1 and the `NovaFinds_Lakeflow_Pipeline.py` in this task solve the same problem at different levels of maturity:

| Dimension | Task 1 (simple_stripe_loader.py) | Task 3 (Lakeflow Pipeline) |
|---|---|---|
| **Ingestion** | Reads local JSON file | Fivetran Starter connector, automatic sync |
| **Stripe → payment mapping** | Explicit Python field mapping | Declarative SQL transformation in Silver layer |
| **Schema evolution** | `ALTER TABLE ADD COLUMN` per run | Unity Catalog governed, automatic |
| **Idempotency** | Application-level `SELECT` check + UNIQUE constraint | DLT exactly-once semantics via checkpointing |
| **Data quality** | None | 12+ EXPECT constraints with hard/soft blocks |
| **Logging** | `print()` statements | Observable pipeline event log, Lakeview dashboard |
| **Alerting** | None | Slack, Email, PagerDuty via DLT notifications |
| **Orchestration** | Run manually | Scheduled, automatically retried on failure |
| **Lineage** | None | Unity Catalog — full column-level lineage |
| **Cost** | Free (local) | ~$135–150/mo (Fivetran) + Serverless compute |

The business logic is the same. The infrastructure around it is production-grade.

---

## How to Deploy

**Prerequisites:** A Databricks workspace with Unity Catalog enabled, a Lakeflow Connect PostgreSQL connection, and a Fivetran Stripe connector configured.

```
1. Upload NovaFinds_Lakeflow_Pipeline.py to your Databricks workspace

2. Create the DLT pipeline:
   Workflows → Delta Live Tables → Create Pipeline
   - Pipeline name: NovaFinds E-Commerce Pipeline
   - Product edition: Advanced (required for data quality metrics)
   - Notebook: select this file
   - Target: main.novafinds
   - Cluster mode: Serverless

3. Update source paths in Bronze layer cells to match your
   Lakeflow Connect and Fivetran catalog/schema names

4. Start the pipeline and verify data quality metrics

5. Schedule the monitoring notebook:
   - Frequency: hourly, 10 minutes after pipeline runs
   - Update pipeline_id in Cell 2 with your pipeline's ID
```

**Finding your pipeline ID:** Open your DLT pipeline in the UI and copy the ID from the URL: `.../delta-live-tables/pipelines/YOUR-ID-HERE`
