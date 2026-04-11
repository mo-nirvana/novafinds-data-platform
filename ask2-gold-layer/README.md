# Task 2 — Gold Layer Analytical Model

**Objective:** Design and implement a Gold layer data model optimised for product profitability, return rates, and regional sales analysis — structured so that Product, Marketing, and Finance teams can answer their questions without writing complex joins.

---

## Two Implementations

This task ships two versions of the same analytical model, which you can choose based on your environment:

| File | Environment | What it does |
|---|---|---|
| `task2_postgresql_pipeline.py` | Local PostgreSQL (Docker) | Runs directly after Task 1 — extends `simple_stripe_loader.py` by building Silver and Gold tables in the same database |
| `task2.ipynb` | Databricks / Jupyter | Full analysis notebook with exploratory charts, currency normalisation, and visualisations of the results |

Both produce the same four Gold tables. The notebook version goes further analytically — it includes the multi-currency USD conversion, data quality exploration, and the visualisations used in the business analysis (regional sales map, product profitability charts, customer order frequency distribution).

---

## How to Run

### Option A — Local PostgreSQL (after Task 1)

**Prerequisites:** Docker running, Task 1 completed (`simple_stripe_loader.py` already run)

```bash
pip install psycopg2-binary pandas

cd task2-gold-layer
python task2_postgresql_pipeline.py
```

The script connects to the same Docker database as Task 1 (port 5431) and builds the full medallion stack on top of the existing tables. On completion it prints an analytics summary — top 5 products by revenue, sales by region, and highest cancellation rate products.

**What it produces:**

```
BRONZE LAYER  → 10 existing raw tables (already in PostgreSQL from Task 1)
SILVER LAYER  → 4 new tables: product_silver, customer_silver,
                               payment_silver, order_silver
GOLD LAYER    → 4 new tables: product_revenue_gold, cancelled_product_gold,
                               sales_region_gold, sales_country_gold
```

### Option B — Databricks / Jupyter Notebook

Upload `task2.ipynb` to a Databricks workspace with the Silver layer tables available in `main.novafinds`, attach to a Serverless cluster, and run all cells. The notebook includes additional steps not in the Python script — see differences below.

---

## Key Differences Between the Two Versions

### Currency normalisation

The notebook version converts all `total_amount` values to USD using static FX rates across 50+ currencies, storing the result as `unified_amount`. This is necessary because NovaFinds operates globally and orders are placed in local currencies — without normalisation, regional sales totals are not comparable.

The PostgreSQL version uses `total_amount` directly without conversion. This means the regional sales figures in `sales_region_gold` mix currencies and should be treated as indicative rather than precise until currency normalisation is added.

### Stripe field integration

The PostgreSQL `payment_silver` explicitly surfaces the Stripe fields added by Task 1 — `stripe_payment_id`, `stripe_status`, and `stripe_currency` — making the pipeline a genuine continuation of the integration work. The notebook version handles this through the Fivetran connector in the Databricks environment.

### `customer_name` and `email`

The PostgreSQL version includes `CONCAT(first_name, ' ', last_name) AS customer_name` and `email` in `customer_silver`. The notebook version omits these for PII reasons in a shared analytical environment.

### `discount_amount`

The PostgreSQL `order_silver` includes `discount_amount` from the orders table. This field is present but not currently used in any Gold layer aggregation — it is surfaced for future analysis once the relationship between discounts and basket value is better understood.

---

## The Business Questions This Model Answers

The Gold layer is designed around three questions the case brief asks directly:

1. **Which products are most profitable?** → `product_revenue_gold`
2. **Which products are returned most often?** → `cancelled_product_gold`
3. **How do sales vary across regions?** → `sales_region_gold` and `sales_country_gold`

Each Gold table is a pre-joined, pre-aggregated answer to one of these questions. A dashboard or analyst can query a Gold table directly without needing to understand the underlying schema.

---

## Modelling Approach: Kimball Dimensional Modelling

The model follows Kimball (star schema) principles. The central fact is `order_silver`, which joins orders with order line items. Dimension tables (`product_silver`, `customer_silver`, `payment_silver`) surround it, and Gold tables are pre-aggregated views of the most common analytical queries.

### Why order line grain, not order grain

An order in NovaFinds can contain multiple products. If the fact table were at the order grain, any product-level analysis (profitability per SKU, return rate per product) would require additional decomposition. The Silver layer join between `orders` and `order_item` brings the grain down to the line level so every metric can be computed directly.

### The `active_status_is_correct` flag

`customer_silver` includes a derived boolean flag computed from the mismatch between `customer.is_active` and `customer_type.type_name`:

```sql
CASE 
  WHEN c.is_active IS true  AND ct.type_name = 'inactive' THEN false
  WHEN c.is_active IS false AND ct.type_name = 'active'   THEN false
  ELSE true
END AS active_status_is_correct
```

This surfaces the 27.34% of customers with conflicting status — a data quality problem that exists in the source system, not a modelling choice. Any query filtering on `is_active` should also filter on `active_status_is_correct = true` until the upstream source of truth is resolved.

---

## Known Limitations

**No sell price per order line** — `product_revenue_gold` uses `price` (the supplier cost recorded in `product`) as the cost basis and `total_amount` / `unified_amount` as the revenue basis. Because one order can contain multiple products at unknown individual prices, the revenue attribution per product is approximate. This is the single most important data gap to resolve — adding a sell price to `order_item` would unlock accurate margin analysis across all 500+ SKUs.

**Static FX rates** (notebook version) — the currency conversion uses hardcoded rates. In production these should be sourced from a live FX API or a managed exchange rate table updated daily.

**No currency normalisation** (PostgreSQL version) — `total_amount` is stored in local currency. Cross-region revenue comparisons using `sales_region_gold` are directionally correct but not precisely comparable until normalisation is added.

---

## Sample Queries

```sql
-- Top 10 products by total revenue
SELECT product_name, category_name, total_revenue, total_quantity
FROM product_revenue_gold
ORDER BY total_revenue DESC
LIMIT 10;

-- Regional performance with average order value
SELECT region, total_sales, order_count,
       ROUND(total_sales / order_count, 2) AS avg_order_value
FROM sales_region_gold
ORDER BY total_sales DESC;

-- Products with highest return rates (minimum 5 orders)
SELECT product_name,
       ROUND(return_rate * 100, 1) AS return_rate_pct,
       cancelled_count,
       non_cancelled_count
FROM cancelled_product_gold
WHERE (cancelled_count + non_cancelled_count) >= 5
ORDER BY return_rate DESC
LIMIT 10;

-- Customers with conflicting status (data quality check)
SELECT COUNT(*) AS flagged_customers,
       ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM customer_silver), 2) AS pct
FROM customer_silver
WHERE active_status_is_correct = false;
```
