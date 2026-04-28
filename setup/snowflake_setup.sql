-- ============================================================
-- Pipelingo demo: Snowflake one-time setup
-- ============================================================
-- Run this in a Snowsight worksheet as ACCOUNTADMIN. It is
-- idempotent — safe to re-run. Creates a least-privileged
-- database, warehouse, role, and TYPE=SERVICE user for the dbt
-- CI to use without ACCOUNTADMIN powers.
--
-- BEFORE RUNNING:
-- 1. Generate a key-pair locally (see setup/generate_keypair.sh).
-- 2. Replace <PASTE_PUBLIC_KEY_HERE> below with the contents of
--    rsa_key.pub MINUS the -----BEGIN/END----- header lines and
--    minus newlines (one long base64 string).
-- ============================================================

USE ROLE ACCOUNTADMIN;

-- Warehouse — XS is enough for this demo and stays well under
-- the trial credit budget. Auto-suspends after 60s of idle.
CREATE WAREHOUSE IF NOT EXISTS PIPELINGO_DEMO_WH
  WITH WAREHOUSE_SIZE = XSMALL
       AUTO_SUSPEND = 60
       AUTO_RESUME = TRUE
       INITIALLY_SUSPENDED = TRUE;

-- Database (dbt creates schemas under it: raw, staging, intermediate, marts)
CREATE DATABASE IF NOT EXISTS PIPELINGO_DEMO;

-- Role with the bare minimum to do dbt's work in this DB
CREATE ROLE IF NOT EXISTS PIPELINGO_DEMO_ROLE;
GRANT USAGE ON WAREHOUSE PIPELINGO_DEMO_WH TO ROLE PIPELINGO_DEMO_ROLE;
GRANT OPERATE ON WAREHOUSE PIPELINGO_DEMO_WH TO ROLE PIPELINGO_DEMO_ROLE;
GRANT USAGE, CREATE SCHEMA ON DATABASE PIPELINGO_DEMO TO ROLE PIPELINGO_DEMO_ROLE;
GRANT ALL ON ALL SCHEMAS IN DATABASE PIPELINGO_DEMO TO ROLE PIPELINGO_DEMO_ROLE;
GRANT ALL ON FUTURE SCHEMAS IN DATABASE PIPELINGO_DEMO TO ROLE PIPELINGO_DEMO_ROLE;
GRANT ALL ON ALL TABLES IN DATABASE PIPELINGO_DEMO TO ROLE PIPELINGO_DEMO_ROLE;
GRANT ALL ON FUTURE TABLES IN DATABASE PIPELINGO_DEMO TO ROLE PIPELINGO_DEMO_ROLE;
GRANT ALL ON ALL VIEWS IN DATABASE PIPELINGO_DEMO TO ROLE PIPELINGO_DEMO_ROLE;
GRANT ALL ON FUTURE VIEWS IN DATABASE PIPELINGO_DEMO TO ROLE PIPELINGO_DEMO_ROLE;

-- Service user (TYPE=SERVICE bypasses Snowflake MFA enforcement
-- since service accounts can't enroll. Auth is via RSA key-pair.)
CREATE USER IF NOT EXISTS PIPELINGO_DBT_SVC
  TYPE = SERVICE
  DEFAULT_ROLE = PIPELINGO_DEMO_ROLE
  DEFAULT_WAREHOUSE = PIPELINGO_DEMO_WH
  DEFAULT_NAMESPACE = PIPELINGO_DEMO.MARTS
  COMMENT = 'CI service account for the pipelingo-demo-dbt project';

-- Attach the public key. Replace the placeholder before running.
-- (You can re-run just this statement to rotate keys later.)
ALTER USER PIPELINGO_DBT_SVC SET RSA_PUBLIC_KEY = '<PASTE_PUBLIC_KEY_HERE>';

GRANT ROLE PIPELINGO_DEMO_ROLE TO USER PIPELINGO_DBT_SVC;

-- Sanity check — should print the new user's RSA fingerprint
DESC USER PIPELINGO_DBT_SVC;
