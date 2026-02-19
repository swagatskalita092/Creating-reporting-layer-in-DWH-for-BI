-- Creating reporting layer in DWH for BI (CRM Subscriptions)
-- Author: Swagat Subhash Kalita
-- Date: 2026-02-18
-- Notes:
-- Clean raw CRM tables into DWH tables, dedupe transactions, log DQ issues,
-- build dm_sales_performance, and compute MRR + cumulative LTV.
-- Assumes raw_customers, raw_subscriptions, raw_transactions already exist.
-- Non-destructive: no DROP; CREATE TABLE IF NOT EXISTS, TRUNCATE before reload, CREATE OR REPLACE VIEW.

CREATE SCHEMA IF NOT EXISTS dwh;

CREATE TABLE IF NOT EXISTS dwh.customers (
    customer_id text PRIMARY KEY,
    company_name text,
    country text,
    signup_date date
);

CREATE TABLE IF NOT EXISTS dwh.subscriptions (
    sub_id text PRIMARY KEY,
    customer_id text,
    plan_type text,
    start_date date,
    end_date date,
    amount numeric(12,2)
);

CREATE TABLE IF NOT EXISTS dwh.transactions_clean (
    tx_id text,
    sub_id text,
    tx_date date,
    status text,
    PRIMARY KEY (tx_id)
);

CREATE TABLE IF NOT EXISTS dwh.dq_issues (
    issue_type text,
    sub_id text,
    customer_id text,
    tx_id text,
    details text,
    created_at timestamptz DEFAULT now()
);

TRUNCATE dwh.transactions_clean, dwh.subscriptions, dwh.customers, dwh.dq_issues RESTART IDENTITY;

INSERT INTO dwh.customers (customer_id, company_name, country, signup_date)
SELECT trim(customer_id), trim(company_name), trim(country), signup_date::date
FROM raw_customers;

INSERT INTO dwh.subscriptions (sub_id, customer_id, plan_type, start_date, end_date, amount)
SELECT trim(sub_id), trim(customer_id), trim(plan_type),
       start_date::date,
       CASE WHEN trim(end_date) = '' OR end_date IS NULL THEN NULL ELSE end_date::date END,
       amount::numeric(12,2)
FROM raw_subscriptions;

INSERT INTO dwh.transactions_clean (tx_id, sub_id, tx_date, status)
SELECT tx_id, sub_id, tx_date, status
FROM (
    SELECT tx_id, trim(sub_id) AS sub_id, tx_date::date AS tx_date, trim(status) AS status,
           row_number() OVER (
               PARTITION BY trim(sub_id), tx_date::date, trim(status)
               ORDER BY
                   (NULLIF(regexp_replace(trim(tx_id), '\D', '', 'g'), '')::bigint) NULLS LAST,
                   tx_id,
                   md5(coalesce(trim(tx_id),'') || coalesce(trim(sub_id),'') || coalesce((tx_date::date)::text,'') || coalesce(trim(status),''))
           ) AS rn
    FROM raw_transactions
) t
WHERE rn = 1;

INSERT INTO dwh.dq_issues (issue_type, sub_id, details)
SELECT 'subscription_end_before_start', s.sub_id,
       'end_date ' || s.end_date::text || ' < start_date ' || s.start_date::text
FROM dwh.subscriptions s
WHERE s.end_date IS NOT NULL AND s.end_date < s.start_date;

INSERT INTO dwh.dq_issues (issue_type, sub_id, tx_id, details)
SELECT 'tx_missing_subscription', t.sub_id, t.tx_id, 'sub_id not in subscriptions'
FROM dwh.transactions_clean t
WHERE NOT EXISTS (SELECT 1 FROM dwh.subscriptions s WHERE s.sub_id = t.sub_id);

INSERT INTO dwh.dq_issues (issue_type, sub_id, customer_id, details)
SELECT 'subscription_missing_customer', s.sub_id, s.customer_id, 'customer_id not in customers'
FROM dwh.subscriptions s
WHERE NOT EXISTS (SELECT 1 FROM dwh.customers c WHERE c.customer_id = s.customer_id);

INSERT INTO dwh.dq_issues (issue_type, sub_id, tx_id, details)
SELECT 'tx_outside_subscription_period', t.sub_id, t.tx_id,
       'tx_date ' || t.tx_date::text || ' outside sub period'
FROM dwh.transactions_clean t
JOIN dwh.subscriptions s ON s.sub_id = t.sub_id
WHERE t.tx_date < s.start_date
   OR (s.end_date IS NOT NULL AND t.tx_date > s.end_date);

CREATE OR REPLACE VIEW dwh.dm_sales_performance AS
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

CREATE INDEX IF NOT EXISTS idx_subscriptions_customer_id ON dwh.subscriptions(customer_id);
CREATE INDEX IF NOT EXISTS idx_subscriptions_dates ON dwh.subscriptions(start_date, end_date);
CREATE INDEX IF NOT EXISTS idx_tx_clean_sub_status ON dwh.transactions_clean(sub_id, status);
CREATE INDEX IF NOT EXISTS idx_tx_clean_date ON dwh.transactions_clean(tx_date);
CREATE INDEX IF NOT EXISTS idx_dq_issue_type ON dwh.dq_issues(issue_type);

-- MRR: month_start, mrr (Annual -> amount/12, Monthly -> amount)
WITH sub_months AS (
    SELECT s.sub_id, s.plan_type, s.amount, g.month_start::date AS month_start
    FROM dwh.subscriptions s
    CROSS JOIN LATERAL generate_series(
        date_trunc('month', s.start_date)::timestamp,
        date_trunc('month', COALESCE(s.end_date, CURRENT_DATE))::timestamp,
        '1 month'::interval
    ) AS g(month_start)
)
SELECT month_start, sum(CASE WHEN plan_type = 'Annual' THEN amount / 12 ELSE amount END) AS mrr
FROM sub_months
GROUP BY month_start
ORDER BY month_start;

-- Cumulative LTV: customer_id, company_name, month_start, monthly_spend, cumulative_ltv
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
           sum(s.amount) AS monthly_spend
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
FROM with_spend
ORDER BY customer_id, month_start;
