-- Creating reporting layer in DWH for BI (CRM Subscriptions)
-- Author: Swagat Subhash Kalita
-- Date: 2026-02-18
-- Notes:
-- Clean raw CRM tables into DWH tables, dedupe transactions, log DQ issues,
-- build dm_sales_performance, and compute MRR + cumulative LTV.

-- ========== SCHEMA & DWH OBJECTS ==========
CREATE SCHEMA IF NOT EXISTS dwh;

-- Cleaned customers
DROP TABLE IF EXISTS dwh.customers CASCADE;
CREATE TABLE dwh.customers (
    customer_id text PRIMARY KEY,
    company_name text,
    country text,
    signup_date date
);

-- Cleaned subscriptions
DROP TABLE IF EXISTS dwh.subscriptions CASCADE;
CREATE TABLE dwh.subscriptions (
    sub_id text PRIMARY KEY,
    customer_id text,
    plan_type text,
    start_date date,
    end_date date,
    amount numeric(12,2)
);

-- Deduped transactions (one row per sub_id, tx_date, status; keep lowest tx_id)
DROP TABLE IF EXISTS dwh.transactions_clean CASCADE;
CREATE TABLE dwh.transactions_clean (
    tx_id text,
    sub_id text,
    tx_date date,
    status text,
    PRIMARY KEY (tx_id)
);

-- DQ issues log
DROP TABLE IF EXISTS dwh.dq_issues CASCADE;
CREATE TABLE dwh.dq_issues (
    issue_type text,
    sub_id text,
    customer_id text,
    tx_id text,
    details text,
    created_at timestamptz DEFAULT now()
);

-- ========== TEST SECTION: RAW TABLES + DATA ==========
DROP TABLE IF EXISTS raw_transactions, raw_subscriptions, raw_customers CASCADE;

CREATE TABLE raw_customers (
    customer_id text,
    company_name text,
    country text,
    signup_date text
);

CREATE TABLE raw_subscriptions (
    sub_id text,
    customer_id text,
    plan_type text,
    start_date text,
    end_date text,
    amount text
);

CREATE TABLE raw_transactions (
    tx_id text,
    sub_id text,
    tx_date text,
    status text
);

-- Customers: c1, c2 (c_missing omitted to trigger DQ)
INSERT INTO raw_customers (customer_id, company_name, country, signup_date) VALUES
('c1', 'Acme Corp', 'USA', '2025-06-01'),
('c2', 'Beta Inc', 'UK', '2025-09-15');

-- Subscriptions
INSERT INTO raw_subscriptions (sub_id, customer_id, plan_type, start_date, end_date, amount) VALUES
('s1', 'c1', 'Annual', '2026-01-15', '2026-12-31', '1200'),
('s2', 'c2', 'Monthly', '2026-02-01', NULL, '100'),
('s_bad_dates', 'c1', 'Monthly', '2026-03-10', '2026-03-01', '100'),
('s_missing_customer', 'c_missing', 'Monthly', '2026-01-01', '2026-01-31', '100');

-- Transactions: duplicates for s1 (same sub_id, tx_date, status -> keep lowest tx_id)
INSERT INTO raw_transactions (tx_id, sub_id, tx_date, status) VALUES
('tx_s1_dup_high', 's1', '2026-01-15', 'Success'),
('tx_s1_dup_low', 's1', '2026-01-15', 'Success'),
('tx_s1_fail', 's1', '2026-02-15', 'Failed'),
('tx_s1_refund', 's1', '2026-03-15', 'Refunded'),
('tx_s2_ok', 's2', '2026-02-01', 'Success'),
('tx_s1_outside', 's1', '2027-01-10', 'Success'),
('tx_missing_sub', 's_missing_sub', '2026-01-01', 'Success');

-- ========== ETL ==========
TRUNCATE dwh.customers, dwh.subscriptions, dwh.transactions_clean, dwh.dq_issues RESTART IDENTITY;

INSERT INTO dwh.customers (customer_id, company_name, country, signup_date)
SELECT
    trim(customer_id),
    trim(company_name),
    trim(country),
    signup_date::date
FROM raw_customers;

INSERT INTO dwh.subscriptions (sub_id, customer_id, plan_type, start_date, end_date, amount)
SELECT
    trim(sub_id),
    trim(customer_id),
    trim(plan_type),
    start_date::date,
    CASE WHEN trim(end_date) = '' OR end_date IS NULL THEN NULL ELSE end_date::date END,
    amount::numeric(12,2)
FROM raw_subscriptions;

-- Dedupe: keep one row per (sub_id, tx_date, status) with lowest tx_id
INSERT INTO dwh.transactions_clean (tx_id, sub_id, tx_date, status)
SELECT tx_id, sub_id, tx_date, status
FROM (
    SELECT tx_id, trim(sub_id) AS sub_id, tx_date::date AS tx_date, trim(status) AS status,
           row_number() OVER (PARTITION BY trim(sub_id), tx_date::date, trim(status) ORDER BY tx_id) AS rn
    FROM raw_transactions
) t
WHERE rn = 1;

-- DQ: subscription end before start
INSERT INTO dwh.dq_issues (issue_type, sub_id, details)
SELECT 'subscription_end_before_start', s.sub_id,
       'end_date ' || s.end_date::text || ' < start_date ' || s.start_date::text
FROM dwh.subscriptions s
WHERE s.end_date IS NOT NULL AND s.end_date < s.start_date;

-- DQ: tx missing subscription
INSERT INTO dwh.dq_issues (issue_type, sub_id, tx_id, details)
SELECT 'tx_missing_subscription', t.sub_id, t.tx_id, 'sub_id not in subscriptions'
FROM dwh.transactions_clean t
WHERE NOT EXISTS (SELECT 1 FROM dwh.subscriptions s WHERE s.sub_id = t.sub_id);

-- DQ: subscription missing customer
INSERT INTO dwh.dq_issues (issue_type, sub_id, customer_id, details)
SELECT 'subscription_missing_customer', s.sub_id, s.customer_id, 'customer_id not in customers'
FROM dwh.subscriptions s
WHERE NOT EXISTS (SELECT 1 FROM dwh.customers c WHERE c.customer_id = s.customer_id);

-- DQ: tx outside subscription period (only for tx that have a subscription)
INSERT INTO dwh.dq_issues (issue_type, sub_id, tx_id, details)
SELECT 'tx_outside_subscription_period', t.sub_id, t.tx_id,
       'tx_date ' || t.tx_date::text || ' outside sub period'
FROM dwh.transactions_clean t
JOIN dwh.subscriptions s ON s.sub_id = t.sub_id
WHERE t.tx_date < s.start_date
   OR (s.end_date IS NOT NULL AND t.tx_date > s.end_date);

-- Data mart view: one row per subscription
DROP VIEW IF EXISTS dwh.dm_sales_performance CASCADE;
CREATE VIEW dwh.dm_sales_performance AS
SELECT
    s.sub_id,
    c.company_name,
    c.country,
    (CASE WHEN s.end_date IS NULL THEN CURRENT_DATE - s.start_date ELSE s.end_date - s.start_date END)::integer AS subscription_duration_days,
    (SELECT count(*) FROM dwh.transactions_clean tc
     WHERE tc.sub_id = s.sub_id AND tc.status = 'Success'
       AND tc.tx_date >= s.start_date
       AND (s.end_date IS NULL OR tc.tx_date <= s.end_date)) AS total_successful_payments
FROM dwh.subscriptions s
LEFT JOIN dwh.customers c ON c.customer_id = s.customer_id;

-- Indexes
CREATE INDEX IF NOT EXISTS idx_subscriptions_customer_id ON dwh.subscriptions(customer_id);
CREATE INDEX IF NOT EXISTS idx_subscriptions_dates ON dwh.subscriptions(start_date, end_date);
CREATE INDEX IF NOT EXISTS idx_tx_clean_sub_status ON dwh.transactions_clean(sub_id, status);
CREATE INDEX IF NOT EXISTS idx_tx_clean_date ON dwh.transactions_clean(tx_date);
CREATE INDEX IF NOT EXISTS idx_dq_issue_type ON dwh.dq_issues(issue_type);

-- ========== ANALYTICS: MRR ==========
-- Month series per sub; Annual -> amount/12, Monthly -> amount per month
WITH sub_months AS (
    SELECT
        s.sub_id,
        s.plan_type,
        s.amount,
        g.month_start::date AS month_start
    FROM dwh.subscriptions s
    CROSS JOIN LATERAL generate_series(
        date_trunc('month', s.start_date)::timestamp,
        date_trunc('month', COALESCE(s.end_date, CURRENT_DATE))::timestamp,
        '1 month'::interval
    ) AS g(month_start)
)
SELECT
    month_start,
    sum(CASE WHEN plan_type = 'Annual' THEN amount / 12 ELSE amount END) AS mrr
FROM sub_months
GROUP BY month_start
ORDER BY month_start;

-- ========== ANALYTICS: CUMULATIVE LTV ==========
WITH cust_months AS (
    SELECT c.customer_id, c.company_name, c.signup_date,
           d.month_start::date AS month_start
    FROM dwh.customers c
    CROSS JOIN LATERAL generate_series(
        date_trunc('month', c.signup_date)::timestamp,
        date_trunc('month', CURRENT_DATE)::timestamp,
        '1 month'::interval
    ) AS d(month_start)
),
spend_amt AS (
    -- We need actual payment amounts; raw has amount on subscription. Use count or subscription amount.
    -- LTV = cumulative spend. Transactions don't have amount; use count of successful txs as proxy
    -- or we need to get amount from subscription. Spec: "total spend grows month-by-month".
    -- So we need revenue per tx. Only subscription has amount; one sub can have many txs.
    -- Approximate: allocate sub amount over its txs for that month, or use count * (amount/period).
    -- Simpler: monthly_spend = sum of successful payment "value". No amount on tx - use subscription amount.
    -- Per sub per month: if we have one success tx, we could allocate that sub's monthly share to that month.
    -- For LTV we want: per customer per month, sum of (value of success txs). So we need to attach amount.
    -- Join tx -> sub: each success tx in a month contributes the sub's monthly equivalent (Annual/12 or Monthly).
    SELECT c.customer_id, date_trunc('month', t.tx_date)::date AS month_start,
           sum(CASE WHEN s.plan_type = 'Annual' THEN s.amount/12 ELSE s.amount END) AS monthly_spend
    FROM dwh.transactions_clean t
    JOIN dwh.subscriptions s ON s.sub_id = t.sub_id
    JOIN dwh.customers c ON c.customer_id = s.customer_id
    WHERE t.status = 'Success'
    GROUP BY c.customer_id, date_trunc('month', t.tx_date)
),
with_spend AS (
    SELECT cm.customer_id, cm.company_name, cm.month_start,
           coalesce(sa.monthly_spend, 0) AS monthly_spend
    FROM cust_months cm
    LEFT JOIN spend_amt sa ON sa.customer_id = cm.customer_id AND sa.month_start = cm.month_start
)
SELECT
    customer_id,
    company_name,
    month_start,
    monthly_spend,
    sum(monthly_spend) OVER (PARTITION BY customer_id ORDER BY month_start) AS cumulative_ltv
FROM with_spend
ORDER BY customer_id, month_start;

-- ========== DEBUG SELECTS (solution.sql only) ==========
-- transactions_clean
SELECT * FROM dwh.transactions_clean ORDER BY sub_id, tx_date, tx_id;

-- dq_issues
SELECT * FROM dwh.dq_issues ORDER BY issue_type, sub_id;

-- dm_sales_performance
SELECT * FROM dwh.dm_sales_performance ORDER BY sub_id;

-- MRR
WITH sub_months AS (
    SELECT s.sub_id, s.plan_type, s.amount, g.month_start::date AS month_start
    FROM dwh.subscriptions s
    CROSS JOIN LATERAL generate_series(
        date_trunc('month', s.start_date)::timestamp,
        date_trunc('month', COALESCE(s.end_date, CURRENT_DATE))::timestamp,
        '1 month'::interval
    ) AS g(month_start)
)
SELECT month_start, sum(CASE WHEN plan_type = 'Annual' THEN amount/12 ELSE amount END) AS mrr
FROM sub_months GROUP BY month_start ORDER BY month_start;

-- LTV
WITH cust_months AS (
    SELECT c.customer_id, c.company_name, d.month_start::date AS month_start
    FROM dwh.customers c
    CROSS JOIN LATERAL generate_series(
        date_trunc('month', c.signup_date)::timestamp,
        date_trunc('month', CURRENT_DATE)::timestamp,
        '1 month'::interval
    ) AS d(month_start)
),
spend_amt AS (
    SELECT c.customer_id, date_trunc('month', t.tx_date)::date AS month_start,
           sum(CASE WHEN s.plan_type = 'Annual' THEN s.amount/12 ELSE s.amount END) AS monthly_spend
    FROM dwh.transactions_clean t
    JOIN dwh.subscriptions s ON s.sub_id = t.sub_id
    JOIN dwh.customers c ON c.customer_id = s.customer_id
    WHERE t.status = 'Success'
    GROUP BY c.customer_id, date_trunc('month', t.tx_date)
),
with_spend AS (
    SELECT cm.customer_id, cm.company_name, cm.month_start,
           coalesce(sa.monthly_spend, 0) AS monthly_spend
    FROM cust_months cm
    LEFT JOIN spend_amt sa ON sa.customer_id = cm.customer_id AND sa.month_start = cm.month_start
)
SELECT customer_id, company_name, month_start, monthly_spend,
       sum(monthly_spend) OVER (PARTITION BY customer_id ORDER BY month_start) AS cumulative_ltv
FROM with_spend ORDER BY customer_id, month_start;

/*
Sanity expectations:
- Annual 1200 contributes 100/month in MRR for months s1 is active (Jan–Dec 2026).
- Duplicates removed: only one Success row for s1 on 2026-01-15 (lowest tx_id kept).
- total_successful_payments counts only Success (s1 has 1, s2 has 1; Failed/Refunded excluded).
- dq_issues contains at least one row for each: subscription_end_before_start, tx_missing_subscription,
  subscription_missing_customer, tx_outside_subscription_period.
*/
