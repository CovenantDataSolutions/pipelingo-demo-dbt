# Pipelingo Demo dbt Project

Realistic e-commerce dbt project for Pipelingo demos. 9 source tables, 16 models total.

## Data Model

**9 source tables** (seeds):
- `raw_customers` — 20 global customers
- `raw_products` — 15 products across 5 categories
- `raw_suppliers` — 10 suppliers across 6 countries
- `raw_orders` — 30 orders over 10 months
- `raw_order_items` — 48 line items
- `raw_payments` — 30 payment records
- `raw_shipments` — 28 shipment records
- `raw_product_reviews` — 18 customer reviews
- `raw_promotions` — 5 promotion codes

**16 models:**
```
Staging (9 views):     stg_customers, stg_products, stg_suppliers, stg_orders,
                       stg_order_items, stg_payments, stg_shipments,
                       stg_product_reviews, stg_promotions

Intermediate (3):      int_orders_enriched       (orders + items + payments + promotions)
                       int_customer_lifetime     (customers + orders + reviews)
                       int_product_performance   (products + suppliers + sales + reviews)

Marts (4):             dim_customers             (with customer_segment)
                       dim_products              (with sales_tier)
                       fct_orders                (order fact with shipment + customer)
                       fct_daily_revenue         (daily rollup)
```

## Setup

### 1. Install dbt Core

```bash
pip install dbt-core dbt-snowflake
```

### 2. Create the Snowflake database

```sql
CREATE DATABASE IF NOT EXISTS PIPELINGO_DEMO;
```

### 3. Configure profiles.yml

```bash
mkdir -p ~/.dbt
cp profiles.yml.example ~/.dbt/profiles.yml
# Edit ~/.dbt/profiles.yml → fill in Snowflake password (or set SNOWFLAKE_PASSWORD env var)
```

### 4. Run

```bash
dbt seed      # load CSVs → raw schema
dbt run       # build all 16 models
dbt test      # run data quality tests
```

This generates `target/manifest.json` and `target/run_results.json`.

## dbt Cloud setup

1. Create a new dbt Cloud project pointed at this repo
2. Configure the Snowflake connection
3. Set up a scheduled Job with commands:
   ```
   dbt seed
   dbt run
   dbt test
   ```
4. After the job runs, download `manifest.json` + `run_results.json` from the Run Details → Artifacts tab

## Upload to Pipelingo

### Option A — Manual upload (one-off)

1. Pipelingo Settings → Connect dbt → **Upload artifacts** tab
2. Select `target/manifest.json` and `target/run_results.json`
3. Upload

### Option B — Automated via GitHub Actions (recommended)

Zero manual uploads. Every push + daily schedule runs dbt and posts results to Pipelingo.

1. **Generate a CI token** in Pipelingo: Settings → CI Automation → "Generate CI token" → copy the `pip_...` value.

2. **Add GitHub Secrets** to this repo (Settings → Secrets and variables → Actions):
   - `PIPELINGO_TOKEN` — the token from step 1
   - `SNOWFLAKE_ACCOUNT` — e.g. `kjc87988.us-east-1`
   - `SNOWFLAKE_USER`
   - `SNOWFLAKE_PASSWORD`
   - `SNOWFLAKE_ROLE` — e.g. `ACCOUNTADMIN`
   - `SNOWFLAKE_DATABASE` — `PIPELINGO_DEMO`
   - `SNOWFLAKE_WAREHOUSE` — `COMPUTE_WH`

3. **The workflow** at `.github/workflows/pipelingo-sync.yml` will now:
   - Run on every push to `main`
   - Run daily at 08:00 UTC (cron)
   - Run manually via "Run workflow" button
   - Execute `dbt seed / run / test`, then POST artifacts to Pipelingo

That's it — your dashboard + lineage stay in sync automatically.

## Demoing a failure

Break a model intentionally, e.g. in `models/marts/dim_customers.sql`:

```sql
-- Bad column name:
from lifetime_BROKEN
```

Run `dbt run --select dim_customers` → it fails. Re-upload `run_results.json` to Pipelingo — Claude AI explains the failure on the dashboard.
