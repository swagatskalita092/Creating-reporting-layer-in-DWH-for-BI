# How to Run This Project

Run the solution, then QA checks. Use Docker (recommended) or local PostgreSQL.

---

## Prerequisites

- **Docker:** [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running, or
- **Local:** PostgreSQL 12+ with `psql` in PATH.

---

## Option A: Docker

### 1. Start the database

```powershell
cd "C:\Users\swaga\OneDrive\Desktop\Creating reporting layer in DWH for BI"
docker compose up -d
```

Wait 5–10 seconds. Optional check: `docker compose exec postgres pg_isready -U dwh -d dwh_bi`

### 2. Load test data and run ETL

**Option 2a — Full test (solution.sql):** Creates raw tables, test data, ETL, and debug output.

```powershell
docker compose exec -T postgres psql -U dwh -d dwh_bi -v ON_ERROR_STOP=1 -f /workspace/solution.sql
```

**Option 2b — Approved ETL only (final_submission.sql):** Requires raw tables. Run after solution.sql once to populate raw, or use your own raw data.

```powershell
docker compose exec -T postgres psql -U dwh -d dwh_bi -v ON_ERROR_STOP=1 -f /workspace/final_submission.sql
```

### 3. Run QA checks

```powershell
docker compose exec -T postgres psql -U dwh -d dwh_bi -v ON_ERROR_STOP=1 -f /workspace/qa_checks.sql
```

Success = table with **OVERALL | PASS | 6 of 6 checks passed**.

### 4. Stop (optional)

```powershell
docker compose down
```

---

## Option B: One script (solution + QA)

```powershell
.\run-checks.ps1
```

Runs `solution.sql` then `qa_checks.sql`. Use for a quick full test.

---

## Option C: Validate final_submission.sql

1. Run **solution.sql** once (creates raw tables + test data).
2. Run **final_submission.sql** (TRUNCATE + reload with approved ETL).
3. Run **qa_checks.sql**.

All 6 checks + OVERALL should be PASS.

---

## Option D: Local PostgreSQL

1. Create database: `dwh_bi`.
2. Create raw tables and load data (or use solution.sql to create + insert test data).
3. Run ETL: `psql -U your_user -d dwh_bi -v ON_ERROR_STOP=1 -f final_submission.sql`
4. Run QA: `psql -U your_user -d dwh_bi -v ON_ERROR_STOP=1 -f qa_checks.sql`

---

## Expected QA output

| check_name | status | meaning |
|------------|--------|--------|
| no_duplicates_tx_clean | PASS | No duplicate (sub_id, tx_date, status) |
| dq_issues_all_four_types | PASS | All 4 DQ issue types present |
| dm_one_row_per_sub_id | PASS | One row per subscription in view |
| mrr_s1_annual_100_per_month | PASS | MRR includes 100/month for annual |
| s1_total_success_excludes_failed_refunded | PASS | Only Success counted for s1 |
| cumulative_ltv_non_decreasing | PASS | LTV non-decreasing per customer |
| **OVERALL** | **PASS** | 6 of 6 checks passed |

---

## Connection (Docker)

- Host: `localhost`  
- Port: `5433`  
- User: `dwh`  
- Database: `dwh_bi`  
- Password: `dwh`

---

## Troubleshooting

- **Port in use:** In `docker-compose.yml` change `"5433:5432"` to e.g. `"5434:5432"`.
- **QA FAIL:** Run solution.sql (or ensure raw data exists), then final_submission.sql, then qa_checks.sql again.
- **ERROR in script:** Check the reported line; ensure raw tables exist when using final_submission.sql.
