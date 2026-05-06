-- ============================================================
-- Pipelingo demo · daily data refresh
-- ============================================================
--
-- WHAT THIS DOES
--   Sets up a Snowflake task that runs every day at 02:00 UTC and
--   inserts ~325 new rows into your raw_payments / raw_shipments /
--   raw_marketing_clicks tables. The new rows reference real TPC-H
--   keys so downstream joins work, and ~5% of generated rows have
--   intentional anomalies (null payment_method, failed status,
--   anonymous clicks) so failures organically appear and you have
--   real cases to test Pipelingo's failure analysis on.
--
-- HOW TO RUN
--   1. Make sure dbt seed has run at least once (creates the
--      raw_payments / raw_shipments / raw_marketing_clicks /
--      raw_marketing_campaigns tables).
--   2. Adjust the database + schema names in the USE statements
--      below to match your dbt target.
--   3. Paste this entire file into a Snowflake worksheet and run.
--      The procedure runs once immediately to verify it works,
--      then the task takes over on the daily schedule.
--
-- COST
--   ~$0.20/month for the daily task itself (X-Small warehouse,
--   ~10 sec per run × 30 days). The resource monitor below caps
--   the entire warehouse at $20/month — you cannot blow past that
--   even if something goes wrong.
--
-- TO TEAR DOWN
--   See the commented-out section at the bottom of this file.

-- ============================================================
-- 1. Adjust these to match your environment
-- ============================================================

USE WAREHOUSE COMPUTE_WH;
USE DATABASE  PIPELINGO_DEMO;   -- adjust if your dbt target uses a different DB
USE SCHEMA    MARTS_RAW;         -- dbt seeds with +schema:raw + target.schema=marts → MARTS_RAW
USE ROLE      ACCOUNTADMIN;      -- or whatever role owns the warehouse + can create tasks


-- ============================================================
-- 2. Safety net — resource monitor so this can never go runaway
-- ============================================================
-- Caps the warehouse at 10 credits/month (~$20 at Standard tier).
-- Hits 75% → notifies. Hits 100% → suspends. 110% → kills mid-query.
-- Even if the task somehow fires every minute instead of daily, the
-- monitor will physically prevent the bill from exceeding $22/month.

CREATE RESOURCE MONITOR IF NOT EXISTS DEV_MONITOR
    WITH CREDIT_QUOTA = 10
    FREQUENCY = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS
        ON 75  PERCENT DO NOTIFY
        ON 100 PERCENT DO SUSPEND
        ON 110 PERCENT DO SUSPEND_IMMEDIATE;

ALTER WAREHOUSE COMPUTE_WH SET RESOURCE_MONITOR = DEV_MONITOR;
ALTER WAREHOUSE COMPUTE_WH SET WAREHOUSE_SIZE = 'XSMALL';
ALTER WAREHOUSE COMPUTE_WH SET AUTO_SUSPEND = 30;            -- seconds idle before suspend
ALTER WAREHOUSE COMPUTE_WH SET AUTO_RESUME   = TRUE;


-- ============================================================
-- 3. The procedure that generates the daily rows
-- ============================================================
-- Idempotent: if it's already run today, it's a no-op.
-- Bounded:    inserts a fixed sample size, can't loop or grow unbounded.

CREATE OR REPLACE PROCEDURE GENERATE_DAILY_RAW_DATA()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    today_str         STRING;
    payment_prefix    STRING;
    shipment_prefix   STRING;
    click_prefix      STRING;
    existing_count    INTEGER;
BEGIN
    today_str       := TO_CHAR(CURRENT_DATE(), 'YYYYMMDD');
    payment_prefix  := 'PMT_'   || today_str || '_';
    shipment_prefix := 'SHIP_'  || today_str || '_';
    click_prefix    := 'CLICK_' || today_str || '_';

    -- Idempotency: if today's batch already exists, exit cleanly.
    SELECT COUNT(*) INTO :existing_count
    FROM RAW_PAYMENTS
    WHERE payment_id LIKE :payment_prefix || '%';

    IF (existing_count > 0) THEN
        RETURN 'No-op — ' || existing_count || ' rows already exist for ' || today_str;
    END IF;

    -- ---- ~100 new payments ----
    -- ~5% have NULL payment_method (will fail dbt tests if any are added)
    -- ~5% are 'failed', ~5% 'pending', ~5% 'refunded', rest 'captured'
    INSERT INTO RAW_PAYMENTS (payment_id, order_id, customer_id, amount_usd, payment_method, status, paid_at)
    WITH sampled AS (
        SELECT o_orderkey, o_custkey,
               ROW_NUMBER() OVER (ORDER BY RANDOM()) AS rn
        FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS
        SAMPLE (100 ROWS)
    )
    SELECT
        :payment_prefix || LPAD(rn::STRING, 4, '0') AS payment_id,
        o_orderkey AS order_id,
        o_custkey  AS customer_id,
        ROUND(UNIFORM(15, 5000, RANDOM())::FLOAT, 2) AS amount_usd,
        CASE
            WHEN UNIFORM(1, 100, RANDOM()) <= 5 THEN NULL  -- 5% null (anomaly)
            ELSE ARRAY_CONSTRUCT('credit_card','debit_card','ach','paypal','apple_pay')
                 [UNIFORM(0, 4, RANDOM())]::VARCHAR
        END AS payment_method,
        CASE
            WHEN UNIFORM(1, 100, RANDOM()) <= 5  THEN 'failed'
            WHEN UNIFORM(1, 100, RANDOM()) <= 5  THEN 'pending'
            WHEN UNIFORM(1, 100, RANDOM()) <= 5  THEN 'refunded'
            ELSE 'captured'
        END AS status,
        DATEADD('hour', -UNIFORM(0, 36, RANDOM()), CURRENT_TIMESTAMP()) AS paid_at
    FROM sampled;

    -- ---- ~75 new shipments ----
    INSERT INTO RAW_SHIPMENTS (shipment_id, order_id, carrier, tracking_number, status, shipped_at, delivered_at)
    WITH sampled AS (
        SELECT o_orderkey,
               ROW_NUMBER() OVER (ORDER BY RANDOM()) AS rn
        FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS
        SAMPLE (75 ROWS)
    )
    SELECT
        :shipment_prefix || LPAD(rn::STRING, 4, '0') AS shipment_id,
        o_orderkey AS order_id,
        ARRAY_CONSTRUCT('UPS','FedEx','USPS','DHL')[UNIFORM(0,3,RANDOM())]::VARCHAR AS carrier,
        UNIFORM(100000000, 999999999, RANDOM())::STRING AS tracking_number,
        CASE
            WHEN UNIFORM(1, 100, RANDOM()) <= 5  THEN 'returned'
            WHEN UNIFORM(1, 100, RANDOM()) <= 15 THEN 'in_transit'
            WHEN UNIFORM(1, 100, RANDOM()) <= 5  THEN 'delayed'
            ELSE 'delivered'
        END AS status,
        DATEADD('day', -UNIFORM(1, 7, RANDOM()), CURRENT_TIMESTAMP()) AS shipped_at,
        CASE
            WHEN UNIFORM(1, 100, RANDOM()) <= 75
                THEN DATEADD('day', UNIFORM(2, 8, RANDOM()), CURRENT_TIMESTAMP())::STRING
            ELSE NULL
        END AS delivered_at
    FROM sampled;

    -- ---- ~150 new clicks ----
    -- ~30% anonymous (null customer_id) — realistic and tests null-handling
    -- ~12% conversion rate (industry-realistic)
    INSERT INTO RAW_MARKETING_CLICKS (click_id, campaign_id, customer_id, clicked_at, source_url, converted)
    WITH sampled AS (
        SELECT c_custkey,
               ROW_NUMBER() OVER (ORDER BY RANDOM()) AS rn
        FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER
        SAMPLE (150 ROWS)
    )
    SELECT
        :click_prefix || LPAD(rn::STRING, 5, '0') AS click_id,
        'CMP_' || LPAD(UNIFORM(1, 40, RANDOM())::STRING, 4, '0') AS campaign_id,
        CASE
            WHEN UNIFORM(1, 100, RANDOM()) <= 70 THEN c_custkey::STRING
            ELSE NULL
        END AS customer_id,
        DATEADD('hour', -UNIFORM(0, 24, RANDOM()), CURRENT_TIMESTAMP()) AS clicked_at,
        '/landing/' || ARRAY_CONSTRUCT('summer-sale','new-arrivals','holiday','clearance','newsletter')
            [UNIFORM(0,4,RANDOM())]::VARCHAR AS source_url,
        CASE WHEN UNIFORM(1, 100, RANDOM()) <= 12 THEN 'true' ELSE 'false' END AS converted
    FROM sampled;

    RETURN 'OK — inserted ~325 rows for ' || today_str;
END;
$$;


-- ============================================================
-- 4. The task — runs the procedure every day at 02:00 UTC
-- ============================================================

CREATE OR REPLACE TASK DAILY_DATA_REFRESH
    WAREHOUSE = COMPUTE_WH
    SCHEDULE  = 'USING CRON 0 2 * * * UTC'
    COMMENT   = 'Generates ~325 realistic new rows in raw_payments/shipments/clicks daily for Pipelingo demo'
AS
    CALL GENERATE_DAILY_RAW_DATA();

-- Tasks are CREATED in SUSPENDED state — must explicitly resume.
ALTER TASK DAILY_DATA_REFRESH RESUME;


-- ============================================================
-- 5. Verify everything is working
-- ============================================================

-- Run the procedure once now to confirm it works (and seed today's batch).
CALL GENERATE_DAILY_RAW_DATA();

-- Check the task is enabled and on schedule
SHOW TASKS LIKE 'DAILY_DATA_REFRESH';

-- Confirm rows landed in the raw tables
SELECT 'payments' AS table_name, COUNT(*) AS row_count FROM RAW_PAYMENTS
UNION ALL
SELECT 'shipments', COUNT(*) FROM RAW_SHIPMENTS
UNION ALL
SELECT 'marketing_clicks', COUNT(*) FROM RAW_MARKETING_CLICKS;

-- Inspect the last 5 generated rows in each table
SELECT * FROM RAW_PAYMENTS         WHERE payment_id  LIKE 'PMT_%'   ORDER BY paid_at    DESC LIMIT 5;
SELECT * FROM RAW_SHIPMENTS        WHERE shipment_id LIKE 'SHIP_%'  ORDER BY shipped_at DESC LIMIT 5;
SELECT * FROM RAW_MARKETING_CLICKS WHERE click_id    LIKE 'CLICK_%' ORDER BY clicked_at DESC LIMIT 5;


-- ============================================================
-- TEAR-DOWN (uncomment to remove everything this script created)
-- ============================================================
-- ALTER TASK DAILY_DATA_REFRESH SUSPEND;
-- DROP TASK IF EXISTS DAILY_DATA_REFRESH;
-- DROP PROCEDURE IF EXISTS GENERATE_DAILY_RAW_DATA();
-- DROP RESOURCE MONITOR IF EXISTS DEV_MONITOR;
