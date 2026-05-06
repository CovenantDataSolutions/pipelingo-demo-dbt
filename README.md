# Pipelingo Demo dbt Project

Realistic e-commerce / SaaS dbt project that runs end-to-end against
Snowflake using the free `SNOWFLAKE_SAMPLE_DATA.TPCH_SF1` benchmark
data plus four supplemental seed tables. Ships its `manifest.json`,
`run_results.json`, and `dbt.log` to a Pipelingo workspace on every
push and on a daily schedule.

## Data model — 8 raw + 8 staging + 3 intermediate + 4 marts + 5 reports = 28 entities

**8 raw entities** (no storage cost — TPC-H is free, seeds are tiny):

| Source | Origin | Rows |
|---|---|---|
| `tpch.customer` | Snowflake free sample data | 150,000 |
| `tpch.orders` | Snowflake free sample data | ~1.5M (1992-1998) |
| `tpch.lineitem` | Snowflake free sample data | ~6M |
| `tpch.part` | Snowflake free sample data | 200,000 |
| `raw_payments` | dbt seed (synthesized) | 4,264 |
| `raw_shipments` | dbt seed (synthesized) | 3,713 |
| `raw_marketing_campaigns` | dbt seed (synthesized) | 40 |
| `raw_marketing_clicks` | dbt seed (synthesized) | 8,000 |

**20 dbt models** layered cleanly:

```
Staging (8 views):       stg_customers, stg_orders, stg_order_items, stg_products,
                         stg_payments, stg_shipments, stg_marketing_campaigns,
                         stg_marketing_clicks

Intermediate (3 ephemeral): int_orders_enriched   (orders + items + payments + shipments)
                            int_customer_lifetime (customers + orders + acquisition)
                            int_product_performance (products + items + sales)

Marts/core (4 tables):   dim_customers, dim_products, fct_orders, fct_revenue

Marts/reports (5 tables): rpt_daily_revenue, rpt_customer_ltv,
                          rpt_marketing_roi, rpt_funnel, rpt_executive_summary
```

Tests in `models/staging/_models.yml` and `models/marts/_models.yml`
cover uniqueness, not-null, and accepted-values on every primary key
and critical column.

## One-time setup

You need: a paid Snowflake account (any tier, $10-15/month for solo dev)
and a Pipelingo workspace.

### 1. Generate a Snowflake key-pair

```bash
bash setup/generate_keypair.sh
```

This drops two files in `setup/keys/` (gitignored). The script prints
the exact public key string you'll paste in the next step.

### 2. Create Snowflake objects (warehouse, database, service user)

Open Snowsight as `ACCOUNTADMIN`, paste the contents of
[`setup/snowflake_setup.sql`](setup/snowflake_setup.sql), replace the
`<PASTE_PUBLIC_KEY_HERE>` placeholder, and run.

This creates idempotently:
- `PIPELINGO_DEMO_WH` — XSMALL warehouse, 30s auto-suspend
- `PIPELINGO_DEMO` — database
- `PIPELINGO_DEMO_ROLE` — least-privileged role
- `PIPELINGO_DBT_SVC` — service user authed by your public key
  (bypasses MFA since service users can't enroll)

### 3. Generate the supplemental seed CSVs

```bash
python3 scripts/generate_seeds.py
```

This regenerates the 4 supplemental seed CSVs with realistic
distributions tied to TPC-H custkeys/orderkeys. Reproducible (uses
a fixed random seed) so the same CSVs come out every time.

### 4. Run dbt locally to verify

```bash
.venv/bin/pip install dbt-core dbt-snowflake
mkdir -p ~/.dbt
cp profiles.yml.example ~/.dbt/profiles.yml
cp setup/keys/rsa_key.p8 ~/.dbt/rsa_key.p8
# Edit ~/.dbt/profiles.yml — fill in account locator (e.g. kjc87988.us-east-1)

dbt seed   # loads 4 CSVs → PIPELINGO_DEMO.raw schema
dbt run    # builds 20 models across staging → intermediate → marts → reports
dbt test   # runs uniqueness + not-null + accepted-values tests
```

Artifacts land in `target/manifest.json` and `target/run_results.json`
— these are what Pipelingo ingests.

### 5. Set up daily realistic data flow (recommended)

For Pipelingo to have *real* daily activity to monitor — failures, new
rows, evolving distributions — set up the Snowflake task that simulates
ongoing ETL:

```sql
-- In Snowsight, paste the contents of:
-- setup/daily_data_refresh.sql
```

This creates:
- A resource monitor capping the warehouse at 10 credits/month (~$20)
- A stored procedure that inserts ~325 new rows daily across raw_payments,
  raw_shipments, raw_marketing_clicks (with ~5% intentional anomalies)
- A task scheduled for 02:00 UTC that calls the procedure

After this is set up, every daily dbt run sees fresh data, alerts fire
on real anomalies, and you have a continuously-evolving warehouse to
test Pipelingo against. **Cost: ~$0.20/month for the task itself.**

### 6. Wire up CI (auto-runs on push + daily)

`.github/workflows/pipelingo-sync.yml` is already in place. Add **7
GitHub secrets** to this repo (Settings → Secrets and variables → Actions):

| Secret | Value |
| --- | --- |
| `SNOWFLAKE_ACCOUNT` | e.g. `kjc87988.us-east-1` |
| `SNOWFLAKE_USER` | `PIPELINGO_DBT_SVC` |
| `SNOWFLAKE_PRIVATE_KEY` | the **full** contents of `setup/keys/rsa_key.p8`, including BEGIN/END lines |
| `SNOWFLAKE_ROLE` | `PIPELINGO_DEMO_ROLE` |
| `SNOWFLAKE_DATABASE` | `PIPELINGO_DEMO` |
| `SNOWFLAKE_WAREHOUSE` | `PIPELINGO_DEMO_WH` |
| `PIPELINGO_TOKEN` | generate at pipelingo.com → Settings → CI Automation |

The workflow fires on:
- every push to `main`
- daily at `08:00 UTC`
- manual `workflow_dispatch`

Each run executes `dbt seed → run → test`, then uploads the artifacts
to Pipelingo. Test failures don't stop the upload (`continue-on-error: true`)
so you still see the failure on your dashboard with AI analysis.

## Demoing a failure

The simplest way to trigger an AI-analyzed failure: break a model
intentionally.

```sql
-- models/staging/stg_payments.sql — change a column reference to one that doesn't exist
cast(payment_date as date) as paid_at,   -- raw_payments has paid_at, not payment_date
```

Push the change, wait for the workflow to run (~2 min), and Pipelingo's
dashboard will show the failure with one-click "Run Technical Analysis"
that reads the compiled SQL and explains the bad reference.

Revert the change to clear the failure on the next run. Or rely on the
~5% organic anomaly rate from the daily data refresh task — failures
will appear naturally without manual intervention.

## Cost

| Component | Monthly |
|---|---|
| Daily data refresh task (X-Small, ~10 sec/day) | ~$0.20 |
| dbt build from CI (X-Small, ~3 min/day) | ~$3.50 |
| Pipelingo's Snowflake query history sync | ~$0.30 |
| Storage (TPC-H is free; tables ~50MB) | <$0.05 |
| Buffer for ad-hoc dev queries | $2-4 |
| **Realistic monthly total** | **~$8-12** |
| **Hard cap from resource monitor** | **$22** |

The resource monitor in `setup/daily_data_refresh.sql` physically
prevents you from being billed past $22/month even if something goes
wrong.

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `dbt seed` fails: "table does not exist" | First run, schemas not yet created | dbt creates them automatically — re-run |
| `Database access error: SNOWFLAKE_SAMPLE_DATA` | Sample data not enabled in your account | Run `SHOW SHARES;` in Snowsight; if missing, follow [Snowflake docs](https://docs.snowflake.com/en/user-guide/sample-data) to mount it |
| Daily task says "no-op already ran today" | Task fired more than once today | Expected — idempotent by design |
| dbt build slow (> 10 min) | Probably querying 6M lineitems on XS | Bump to S warehouse for `dbt run` only, or filter `stg_order_items` to a date range |
