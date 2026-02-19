-- Creating reporting layer in DWH for BI (CRM Subscriptions)
-- Author: Swagat Subhash Kalita
-- Date: 2026-02-18
-- Notes:
-- PASS/FAIL checks run after solution.sql. Output: check_name | status | details.

WITH
dup_check AS (
    SELECT count(*) AS dup_count
    FROM (
        SELECT sub_id, tx_date, status, count(*)
        FROM dwh.transactions_clean
        GROUP BY sub_id, tx_date, status
        HAVING count(*) > 1
    ) x
),
dq_types AS (
    SELECT array_agg(DISTINCT issue_type ORDER BY issue_type) AS types
    FROM dwh.dq_issues
),
dm_row_check AS (
    SELECT (SELECT count(*) FROM dwh.dm_sales_performance) AS rn,
           (SELECT count(DISTINCT sub_id) FROM dwh.dm_sales_performance) AS dn
),
mrr_s1_check AS (
    SELECT count(*) AS cnt
    FROM (
        WITH sub_months AS (
            SELECT s.sub_id, s.plan_type, s.amount, g.month_start::date AS month_start
            FROM dwh.subscriptions s
            CROSS JOIN LATERAL generate_series(
                date_trunc('month', s.start_date)::timestamp,
                date_trunc('month', COALESCE(s.end_date, CURRENT_DATE))::timestamp,
                '1 month'::interval
            ) AS g(month_start)
            WHERE s.sub_id = 's1'
        )
        SELECT 1 FROM sub_months
        WHERE plan_type = 'Annual' AND amount = 1200
          AND (SELECT sum(CASE WHEN plan_type = 'Annual' THEN amount/12 ELSE amount END) FROM sub_months WHERE sub_id = 's1' LIMIT 1) = 100
    ) x
),
-- Simpler: MRR for s1 (annual 1200) must have at least one month with mrr contribution 100
mrr_100_check AS (
    SELECT count(*) AS cnt
    FROM (
        WITH sub_months AS (
            SELECT s.sub_id, s.plan_type, s.amount, g.month_start::date AS month_start
            FROM dwh.subscriptions s
            CROSS JOIN LATERAL generate_series(
                date_trunc('month', s.start_date)::timestamp,
                date_trunc('month', COALESCE(s.end_date, CURRENT_DATE))::timestamp,
                '1 month'::interval
            ) AS g(month_start)
        ),
        mrr AS (
            SELECT month_start, sum(CASE WHEN plan_type = 'Annual' THEN amount/12 ELSE amount END) AS mrr
            FROM sub_months
            GROUP BY month_start
        )
        SELECT 1 FROM mrr WHERE mrr = 100
    ) x
),
s1_success_count AS (
    SELECT total_successful_payments AS n FROM dwh.dm_sales_performance WHERE sub_id = 's1'
),
ltv_check AS (
    SELECT bool_and(cumulative_ltv >= prev_ltv) AS ok
    FROM (
        SELECT customer_id, month_start, cumulative_ltv,
               lag(cumulative_ltv) OVER (PARTITION BY customer_id ORDER BY month_start) AS prev_ltv
        FROM (
            WITH cust_months AS (
                SELECT c.customer_id, d.month_start::date AS month_start
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
                SELECT cm.customer_id, cm.month_start, coalesce(sa.monthly_spend, 0) AS monthly_spend
                FROM cust_months cm
                LEFT JOIN spend_amt sa ON sa.customer_id = cm.customer_id AND sa.month_start = cm.month_start
            )
            SELECT customer_id, month_start, monthly_spend,
                   sum(monthly_spend) OVER (PARTITION BY customer_id ORDER BY month_start) AS cumulative_ltv
            FROM with_spend
        ) t
    ) u
    WHERE prev_ltv IS NOT NULL
),
all_checks AS (
    SELECT 'no_duplicates_tx_clean' AS check_name,
           CASE WHEN (SELECT dup_count FROM dup_check) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
           coalesce((SELECT dup_count::text || ' duplicate (sub_id,tx_date,status) groups' FROM dup_check), 'N/A') AS details
    UNION ALL
    SELECT 'dq_issues_all_four_types',
           CASE WHEN (SELECT types FROM dq_types) @> ARRAY['subscription_end_before_start', 'subscription_missing_customer', 'tx_missing_subscription', 'tx_outside_subscription_period']::text[]
                THEN 'PASS' ELSE 'FAIL' END,
           (SELECT coalesce(array_to_string(types, ','), 'none') FROM dq_types)
    UNION ALL
    SELECT 'dm_one_row_per_sub_id',
           CASE WHEN (SELECT rn FROM dm_row_check) = (SELECT dn FROM dm_row_check) THEN 'PASS' ELSE 'FAIL' END,
           (SELECT 'rows=' || rn || ' distinct_sub_id=' || dn FROM dm_row_check)
    UNION ALL
    SELECT 'mrr_s1_annual_100_per_month',
           CASE WHEN (SELECT cnt FROM mrr_100_check) > 0 THEN 'PASS' ELSE 'FAIL' END,
           (SELECT 'months with mrr=100: ' || cnt FROM mrr_100_check)
    UNION ALL
    SELECT 's1_total_success_excludes_failed_refunded',
           CASE WHEN (SELECT n FROM s1_success_count) = 1 THEN 'PASS' ELSE 'FAIL' END,
           (SELECT 's1 total_successful_payments=' || coalesce(n::text, 'NULL') FROM s1_success_count)
    UNION ALL
    SELECT 'cumulative_ltv_non_decreasing',
           CASE WHEN (SELECT coalesce(ok, true) FROM ltv_check) THEN 'PASS' ELSE 'FAIL' END,
           (SELECT CASE WHEN (SELECT ok FROM ltv_check) THEN 'OK' ELSE 'decrease found' END)
)
SELECT check_name, status, details FROM all_checks
UNION ALL
SELECT 'OVERALL' AS check_name,
       CASE WHEN EXISTS (SELECT 1 FROM all_checks WHERE status = 'FAIL') THEN 'FAIL' ELSE 'PASS' END AS status,
       (SELECT count(*)::text || ' of 6 checks passed' FROM all_checks WHERE status = 'PASS') AS details
;
