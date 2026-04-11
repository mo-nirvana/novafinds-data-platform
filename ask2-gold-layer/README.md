# Task 2 — Gold Layer Analytical Model

**Objective:** Design a Gold layer data model optimised for product profitability, return rates, and regional sales analysis — structured so that Product, Marketing, and Finance teams can answer their questions without writing complex joins.

---

## The Business Questions This Model Answers

The Gold layer is designed around three questions the case brief asks directly:

1. **Which products are most profitable?** → `product_profit_gold`
2. **Which products are returned most often?** → `cancelled_product_gold`
3. **How do sales vary across regions?** → `sales_region_gold` and `sales_country_gold`

Each Gold table is a pre-joined, pre-aggregated answer to one of these questions. A dashboard or analyst should be able to query a Gold table directly without needing to understand the underlying schema.

---

## Modelling Approach: Kimball Dimensional Modelling

The model follows Kimball (star schema) principles. A central fact table records what happened at the most granular useful level, surrounded by dimension tables that describe the who, what, where, and when.

### Fact Table: `fact_order_line`

The grain is **one row per order line item** — not per order. This is a deliberate choice.

An order in NovaFinds can contain multiple products. If the fact table were at the order grain, any product-level analysis (profitability per SKU, return rate per product) would require additional decomposition. By going to the line level, every metric can be computed directly.

```
fact_order_line
├── order_line_key      (surrogate PK)
├── order_key           (FK → dim_order)
├── product_key         (FK → dim_product)
├── customer_key        (FK → dim_customer)
├── payment_key         (FK → dim_payment)
├── geo_key             (FK → dim_geography)
├── date_key            (FK → dim_date)
├── quantity
├── revenue             (unified_amount in USD)
├── cost                (product cost price)
└── gross_margin        (revenue - cost) / revenue
```

### Dimension Tables

| Dimension | Key fields | Why it exists |
|---|---|---|
| `dim_product` | `product_id`, `category_name`, `cost_price` | Enables filtering and grouping by category, brand, and cost |
| `dim_customer` | `customer_id`, `country`, `customer_type` | Enables segmentation by geography and customer tier |
| `dim_geography` | `country`, `region`, `apac_flag` | Supports regional rollups without repeating country-to-region mapping logic across every query |
| `dim_payment` | `stripe_payment_id`, `status`, `currency`, `method` | Connects payment outcomes to order lines for payment success analysis |
| `dim_date` | `date_key`, `day`, `week`, `month`, `quarter`, `year` | Standard time intelligence dimension |

### Gold Aggregation Tables

Rather than expecting analysts to aggregate from the fact table every time, four pre-computed Gold tables serve the primary use cases:

**`product_profit_gold`** — total revenue, total cost, total profit, unit profit, and profit margin per product and category. Note: profit calculations use `price` as the cost basis (the product cost recorded in the source system). True gross margin requires sell price per order line, which is not currently available in the source data — this is documented as a known limitation.

**`cancelled_product_gold`** — cancelled vs non-cancelled order counts per product, with a return rate percentage. Useful for identifying quality or fulfilment issues at the SKU level.

**`sales_region_gold`** — total sales, order count, and customer count aggregated by region (APAC, EMEA, LATAM, North America). The region mapping is centralised in this table so the definition of "which countries belong to APAC" is consistent across all reports.

**`sales_country_gold`** — the same metrics at country granularity for geographic drill-down.

---

## A Note on Data Quality

The Gold layer inherits the data quality limitations of the source data. Two are worth calling out explicitly:

**No sell price per order line** — the `product_profit_gold` table uses `product.price` (the supplier cost) as the cost basis and `unified_amount` (total order revenue) as the revenue basis. Because one order can contain multiple products at unknown individual prices, the revenue attribution per product is approximate. This is documented in the notebook and should be resolved by adding `sell_price` to the order line items in the source system.

**27.34% of customers have conflicting active/inactive status** — the `dim_customer` dimension reflects this directly from the source. Queries filtering on `customer.active` may produce misleading results until a single source of truth is established upstream.

Both limitations are data quality problems to fix at the Silver layer, not modelling problems. The Gold layer design is correct — it will produce accurate results as soon as the source data is clean.

---

## How to Run

The Gold layer model is implemented as a Databricks notebook (`task2.ipynb`) designed to run against the Silver layer tables produced by Task 3's Lakeflow pipeline.

**Prerequisites:** A Databricks workspace with the Silver layer tables available in `main.novafinds`.

```
1. Upload task2.ipynb to your Databricks workspace
2. Attach to a cluster (Serverless recommended)
3. Run all cells
4. Gold tables are written to main.novafinds
```

For local exploration, the notebook can also be run against the Docker PostgreSQL database from Task 1 with minor connection adjustments.

---

## Query Examples

```sql
-- Top 10 products by total profit
SELECT product_name, category_name, total_profit, profit_margin
FROM main.novafinds.product_profit_gold
ORDER BY total_profit DESC
LIMIT 10;

-- Regional performance comparison
SELECT region, total_sales, order_count, customer_count,
       ROUND(total_sales / order_count, 2) AS avg_order_value
FROM main.novafinds.sales_region_gold
ORDER BY total_sales DESC;

-- Products with highest return rates
SELECT product_name, cancelled_count, non_cancelled_count,
       ROUND(return_rate * 100, 1) AS return_rate_pct
FROM main.novafinds.cancelled_product_gold
WHERE (cancelled_count + non_cancelled_count) >= 10  -- minimum volume filter
ORDER BY return_rate DESC
LIMIT 10;
```
