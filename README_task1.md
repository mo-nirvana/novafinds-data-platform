# Task 1 — Stripe Payment Integration

**Objective:** Ingest Stripe API payment data into the existing PostgreSQL `payment` table, preserving legacy dashboard compatibility while capturing new Stripe-specific fields.

---

## The Problem

NovaFinds has officially migrated to Stripe for payment processing. The `payment` table in PostgreSQL still exists and is actively queried by business dashboards. A naive migration — replacing or restructuring the table — would break all existing reports overnight.

The challenge is therefore not just ingestion. It is **zero-disruption integration**: map Stripe's data model onto the existing schema, preserve what dashboards rely on, and store everything Stripe provides without losing any of it.

---

## Design Decisions

### 1. Legacy schema preservation

The existing `payment` table has columns like `amount` and `payment_date` that dashboards query directly. Rather than touching these, the pipeline maps Stripe fields onto them:

| Stripe field | Maps to | Notes |
|---|---|---|
| `amount_received` (cents) | `amount` (dollars) | Divided by 100 |
| `created` (Unix timestamp) | `payment_date` (datetime) | Converted via `datetime.fromtimestamp()` |

No existing column is modified or removed. All legacy queries continue to work.

### 2. Schema evolution via JSONB

Stripe provides rich metadata that does not exist in the current schema — `cancellation_reason`, `payment_error`, `metadata`, and others. Rather than adding a new column per field (which would require a migration for every Stripe API change), new fields are stored in a `stripe_metadata` JSONB column.

This means:
- Zero breaking changes to existing queries
- Full fidelity — no Stripe data is discarded
- Future fields are automatically captured without schema migrations
- Downstream consumers can query individual fields using PostgreSQL's `->` and `->>` JSONB operators

New typed columns (`stripe_payment_id`, `stripe_status`, `stripe_currency`, etc.) are added only for fields that will be queried frequently and benefit from indexing.

### 3. Idempotency

The pipeline is safe to run multiple times. Before inserting a record, it checks whether a payment with that `stripe_payment_id` already exists:

```python
cur.execute(
    "SELECT payment_id FROM payment WHERE stripe_payment_id = %s",
    (stripe_id,)
)
if cur.fetchone():
    skipped += 1
    continue
```

The `stripe_payment_id` column also carries a `UNIQUE` constraint at the database level, providing a hard guarantee against duplicates even if the application-level check is bypassed.

### 4. Logging and observability

Every run prints a structured summary:

```
Total payments read:    50
Payments inserted:      48
Payments skipped:       2     ← already existed (idempotent re-run)
Errors:                 0
```

Individual errors are captured per record without halting the pipeline — a single malformed payment does not abort the entire batch.

---

## How to Run

**Prerequisites:** Docker (for the PostgreSQL database), Python 3.8+

```bash
# From the repository root, start the database
docker compose up -d

# Install the dependency
pip install psycopg2-binary

# Place the Stripe JSON file in this directory
# Expected filename: 20251230_stripe_payments.json
# Expected format: { "data": [ { "id": "pi_...", ... }, ... ] }

# Run the pipeline
cd task1-stripe-integration
python simple_stripe_loader.py
```

**Connection settings** (pre-configured for the Docker container):

| Setting | Value |
|---|---|
| Host | `localhost` |
| Port | `5431` |
| Database | `novafinds_db` |
| User | `novafinds_user` |

---

## Known Limitations

**`payment_id` generation uses `MAX(payment_id) + 1`** — this is not safe under concurrent writes. In production this should be replaced with a PostgreSQL sequence or the table should use `SERIAL` / `BIGSERIAL`.

**No currency normalisation** — Stripe payments may be in multiple currencies. `amount` is stored as-is from `amount_received` divided by 100, without converting to a base currency. Cross-currency revenue aggregation will produce incorrect totals until a normalisation step is added.

**File-based input** — the current implementation reads from a local JSON file rather than calling the Stripe API directly. This is appropriate for the case study scope; production would poll the Stripe API with cursor-based pagination.

---

## How This Evolves in Task 3

In the enterprise platform (Task 3), this script is replaced entirely by managed connectors:

| Current (Task 1) | Enterprise (Task 3) |
|---|---|
| Local Python script | Lakeflow Pipeline (declarative DLT) |
| Manual `ALTER TABLE` column additions | Unity Catalog governed schema evolution |
| `print()` logging | Observable pipeline metrics + Slack/PagerDuty alerting |
| File-based Stripe JSON | Fivetran Stripe connector (automatic sync) |
| `MAX(id) + 1` ID generation | Managed Delta table identity columns |
| Run manually | Scheduled, orchestrated, automatically retried |

The business logic — field mapping, idempotency, metadata preservation — remains the same. The infrastructure around it becomes production-grade.
