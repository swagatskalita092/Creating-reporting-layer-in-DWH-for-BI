# Problem Statement: Creating Reporting Layer in DWH for BI

**Project:** Creating reporting layer in Data Warehouse (DWH) for Business Intelligence (BI) — CRM subscriptions.

---

## Task #1 (Written)

**Question:** Why are you interested in this project and how do you see its implementation?

*(To be filled by the candidate.)*

---

## Task #2: Technical

### 1. Source Data Description

You have **3 tables** from a CRM system:

**raw_subscriptions**

| Column       | Description                              |
|-------------|------------------------------------------|
| sub_id      | Unique subscription identifier           |
| customer_id | Link to the customer                      |
| plan_type   | `'Monthly'` or `'Annual'`                 |
| start_date  | Subscription start date                   |
| end_date    | Expiration date (can be null if active)   |
| amount      | Total price paid for the period           |

**raw_customers**

| Column       | Description                |
|-------------|----------------------------|
| customer_id | Unique identifier          |
| company_name| Name of the client company |
| country     | Country of origin          |
| signup_date | Date the account was created |

**raw_transactions**

| Column  | Description                    |
|---------|--------------------------------|
| tx_id   | Unique transaction ID          |
| sub_id  | Link to the subscription       |
| tx_date | Date of payment                |
| status  | `'Success'`, `'Failed'`, or `'Refunded'` |

---

### 2. The Tasks

#### Task 1: Data Cleaning & Modeling (ETL)

Write SQL scripts to move data from the `raw_` tables into a structured Data Warehouse (DWH) schema. The process should:

- **Handle duplicates:** Identify and remove duplicate records in transactions (e.g. one row per `(sub_id, tx_date, status)`, keeping the row with the lowest `tx_id`).
- **Data quality:** Create Data Quality checks to alert about problems in data (e.g. end_date &lt; start_date, missing subscription/customer, transaction outside subscription period).
- **Data mart:** Build a final table or view named **dm_sales_performance** that joins these sources. Each row should represent **one subscription** and include:
  - Customer name and country  
  - Subscription duration  
  - Total successful payments associated with it (only in-period Success transactions).

#### Task 2: Advanced Analytical SQL

Using the processed tables, write queries to calculate:

1. **MRR (Monthly Recurring Revenue)**  
   Revenue for each month.  
   **Note:** Annual plans (e.g. $1200) should be divided by 12 and spread across the months (e.g. $100/month) to reflect true monthly revenue.

2. **Cumulative LTV (Lifetime Value)**  
   Cumulative LTV for each customer, showing how their total spend grows month-by-month starting from their `signup_date`.

---

### 3. Requirements & Submission

- **Environment:** Use any SQL dialect (this solution uses **PostgreSQL**).
- **Time limit:** Expected 4–8 hours.

---

## Deliverables (as implemented)

| File                  | Purpose                                                                 |
|-----------------------|-------------------------------------------------------------------------|
| **solution.sql**      | ETL + analytics + test section; runnable end-to-end with test data     |
| **qa_checks.sql**     | PASS/FAIL checks that validate requirements after solution runs        |
| **final_submission.sql** | Same ETL + analytics; no test tables/inserts; assumes raw tables exist |
