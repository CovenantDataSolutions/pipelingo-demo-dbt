# Pipelingo Demo dbt Project

Realistic e-commerce dbt project that runs end-to-end against Snowflake's free
tier and ships its `manifest.json` + `run_results.json` to a Pipelingo workspace
on every push.

## Data model

**9 source tables** (CSV seeds, ~213 rows total):
- `raw_customers` — 20 global customers
- `raw_products` — 15 products across 5 categories
- `raw_suppliers` — 10 suppliers across 6 countries
- `raw_orders` — 30 orders over 10 months
- `raw_order_items` — 48 line items
- `raw_payments` — 30 payment records
- `raw_shipments` — 28 shipment records
- `raw_product_reviews` — 18 customer reviews
- `raw_promotions` — 5 promotion codes

**16 models, layered:**
```
Staging (9 views):    stg_customers, stg_products, stg_suppliers, stg_orders,
                      stg_order_items, stg_payments, stg_shipments,
                      stg_product_reviews, stg_promotions

Intermediate (3):     int_orders_enriched      (orders + items + payments + promotions)
                      int_customer_lifetime    (customers + orders + reviews)
                      int_product_performance  (products + suppliers + sales + reviews)

Marts (4):            dim_customers            (with customer_segment)
                      dim_products             (with sales_tier)
                      fct_orders               (order fact + shipment + customer)
                      fct_daily_revenue        (daily rollup)
```

Tests in `models/schema.yml` cover uniqueness and not-null on every primary key.

## One-time setup

You need: a Snowflake account (free trial works) and a Pipelingo workspace.

### 1. Generate a Snowflake key-pair

```bash
bash setup/generate_keypair.sh
```

This drops two files in `setup/keys/` (gitignored). The script also prints the
exact strings you'll paste in the next two steps.

### 2. Create the Snowflake objects

Open Snowsight as `ACCOUNTADMIN`, paste the contents of
[`setup/snowflake_setup.sql`](setup/snowflake_setup.sql), and replace the
`<PASTE_PUBLIC_KEY_HERE>` placeholder with the public-key string the script
printed.

It creates (idempotently):
- `PIPELINGO_DEMO_WH` — XSMALL warehouse with 60s auto-suspend (basically free)
- `PIPELINGO_DEMO` — database (dbt creates `raw` / `staging` / `intermediate` / `marts` schemas under it)
- `PIPELINGO_DEMO_ROLE` — least-privileged role
- `PIPELINGO_DBT_SVC` — `TYPE=SERVICE` user authed by your public key.
  This **bypasses Snowflake MFA enforcement** since service users can't enroll.

### 3. Run dbt locally (optional, but recommended for first verification)

```bash
pip install dbt-core dbt-snowflake
mkdir -p ~/.dbt
cp profiles.yml.example ~/.dbt/profiles.yml
cp setup/keys/rsa_key.p8 ~/.dbt/rsa_key.p8
# Edit ~/.dbt/profiles.yml — fill in your account locator (e.g. kjc87988.us-east-1)

dbt seed       # loads CSVs → PIPELINGO_DEMO.raw schema
dbt run        # builds 16 models across staging/intermediate/marts
dbt test       # runs uniqueness + not-null tests
```

Artifacts land in `target/manifest.json` and `target/run_results.json` — these are what Pipelingo ingests.

### 4. Wire up CI (auto-runs on push + daily)

`.github/workflows/pipelingo-sync.yml` is already in place. Add **7 GitHub
secrets** to this repo (Settings → Secrets and variables → Actions):

| Secret | Value |
| --- | --- |
| `SNOWFLAKE_ACCOUNT` | e.g. `kjc87988.us-east-1` |
| `SNOWFLAKE_USER` | `PIPELINGO_DBT_SVC` |
| `SNOWFLAKE_PRIVATE_KEY` | the **full** contents of `setup/keys/rsa_key.p8`, including `-----BEGIN PRIVATE KEY-----` and `-----END PRIVATE KEY-----` lines |
| `SNOWFLAKE_ROLE` | `PIPELINGO_DEMO_ROLE` |
| `SNOWFLAKE_DATABASE` | `PIPELINGO_DEMO` |
| `SNOWFLAKE_WAREHOUSE` | `PIPELINGO_DEMO_WH` |
| `PIPELINGO_TOKEN` | generate at Pipelingo → Settings → CI Automation |

The workflow fires on:
- every push to `main`
- daily at `08:00 UTC` (the `cron` line)
- manual `workflow_dispatch`

Each run executes `dbt seed → run → test`, then uploads the resulting artifacts
to Pipelingo. Test failures don't stop the upload (`continue-on-error: true`)
so you still see the failure on your dashboard with AI analysis.

### 5. Verify it works

After the first push, in your Pipelingo dashboard:
- 9 source tables show up under `raw`
- 16 pipeline runs appear (one per model + tests)
- Lineage page renders the layered DAG
- Click any test failure → `Run Technical Analysis` → Claude reads the compiled SQL

## Demoing a failure

The simplest way to demo Pipelingo's AI analysis: break a model intentionally.

```sql
-- models/marts/dim_customers.sql
from {{ ref('int_customer_lifetime_BROKEN') }}
```

Push the change, wait for the workflow to run (~2 min), and Pipelingo's
dashboard will show the failure with a one-click `Run Technical Analysis` that
reads the compiled SQL and explains the bad reference.

Revert the change to clear the failure on the next run.

## Cost

The XSMALL warehouse with 60s auto-suspend uses ~0.05 credits per `dbt build` —
about 1¢ at standard pricing. A daily schedule for a year stays under $5.
Snowflake's free trial gives you $400 credit, so this is effectively free
during your trial.
