# Creating Reporting Layer in DWH for BI (CRM Subscriptions)

A Data Warehouse reporting layer that cleans CRM subscription data, enforces data quality, and exposes a sales-performance data mart plus MRR and cumulative LTV analytics. Implemented in **PostgreSQL**.

**Author:** Swagat Subhash Kalita

---

## What this project does

- **ETL:** Loads and cleans data from raw CRM tables (`raw_customers`, `raw_subscriptions`, `raw_transactions`) into a `dwh` schema. Non-destructive: uses `CREATE TABLE IF NOT EXISTS`, `TRUNCATE` before reload, `CREATE OR REPLACE VIEW`.
- **Deduplication:** One row per (sub_id, tx_date, status); deterministic tie-break (numeric from tx_id, then tx_id, then md5). No DROP of DWH tables.
- **Data quality:** Logs four issue types in `dwh.dq_issues` (end_date &lt; start_date, missing subscription, missing customer, tx outside subscription period). Does not block loading.
- **Data mart:** View `dwh.dm_sales_performance` — one row per subscription: company name, country, subscription duration, total successful in-period payments.
- **Analytics:** MRR by month (Annual = amount/12), and cumulative LTV per customer from signup.

---

## Repository structure

| File | Purpose |
|------|--------|
| **final_submission.sql** | **Main deliverable.** ETL + DQ + data mart + MRR + LTV. Non-destructive; assumes raw tables exist. Use for submission or production. |
| **solution.sql** | Full runnable script with test data: creates raw tables, inserts test data, runs ETL, debug output. Use to test end-to-end locally. |
| **qa_checks.sql** | Validation: 6 PASS/FAIL checks + OVERALL. Run after ETL (solution or final_submission). |
| **docker-compose.yml** | PostgreSQL 16 in Docker (port 5433). |
| **run-checks.ps1** | PowerShell: starts DB, runs `solution.sql`, then `qa_checks.sql`. |
| **PROBLEM-STATEMENT.md** | Problem statement and requirements. |
| **HOW-TO-RUN.md** | How to run (Docker and local) and validate. |

---

## How to run

- **Validate final_submission.sql (recommended):** Run `solution.sql` (creates raw + test data), then `final_submission.sql`, then `qa_checks.sql`. All checks should PASS.
- **Quick test:** Run `.\run-checks.ps1` (runs solution.sql + qa_checks.sql). Success = **OVERALL | PASS**.
- **Detailed steps:** See [HOW-TO-RUN.md](HOW-TO-RUN.md).

---

## Requirements

- PostgreSQL 12+ (or Docker). For Docker: [Docker Desktop](https://www.docker.com/products/docker-desktop/).
