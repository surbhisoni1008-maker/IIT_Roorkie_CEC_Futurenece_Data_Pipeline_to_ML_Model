# NammaMart ETL & ELT — Submission notes
**Owner:** Surbhi Soni  
**Repository:** https://github.com/surbhisoni1008-maker/IIT_Roorkie_CEC_Futurenece_Data_Pipeline_to_ML_Model  
**Status:** Core DAG execution and ELT idempotency have been verified locally. Observability experiment log is partially updated; remaining experiments and screenshot evidence are still pending.

## Architecture
- ETL DAG: \`nammamart_etl_surbhi\`; validates orders in Python, quarantines rejected orders, masks customer email/phone in reporting dimensions, and writes a DuckDB star-schema-style fact and summary tables.
- ELT DAG: \`nammamart_elt_surbhi\`; preserves raw exports with ingestion metadata, uses DuckDB SQL for staging and marts, and processes the increment file with newest-row-wins staging deduplication.
- Warehouse: \`include/warehouse_surbhi.duckdb\`. Source CSVs are read-only inputs.
- Schedule: \`0 9 * * 1-6\`, timezone \`Asia/Kolkata\`; 09:00 IST is 03:30 UTC. \`catchup=False\` avoids backfilling old scheduled runs. If Docker/laptop is off, local Airflow cannot run on time; start it and trigger/recover the missed run.

## Part 0 — Data profiling and issue log
Run the DAGs to generate issue reports under \`include/reports/\`. The profiler checks nulls and duplicate keys for all six files; order validation checks required identifiers, quantity, status, timestamp, discount, calculated amount, master keys, delivered-order duration, and future timestamps. Expand this log after inspecting all source-level anomalies.

| Question to clarify with client | Why it matters |
|---|---|
| Does a negative quantity represent a return, or always a data-entry error? | Prevents incorrect rejection or revenue calculation. |
| Should a RETURNED order reverse revenue in the original order date or in the return date? | Determines historical finance reporting and restatement policy. |
| What tolerance and matching policy should Finance use for split/partial payments? | Prevents false reconciliation failures. |

## Part C — Scheduling
- Cron: \`0 9 * * 1-6\` (Monday–Saturday at 09:00).
- Explicit timezone: \`Asia/Kolkata\`; 09:00 IST = 03:30 UTC.
- UTC start date without a timezone would shift the intended local execution time by 5 hours 30 minutes.
- Local Airflow requires the computer and Docker to be running. For production, use a continuously available host and alert on missed/failed runs.

## Part E — Governance
1. **Ownership:** Operations owns orders and store master; Finance owns payment records and reconciliation policy; CRM owns customer records; Merchandising owns product catalogue; Analytics/Data Engineering owns warehouse transformations. Business owners approve rule changes with Finance/Ops sign-off.
2. **Dictionary:** \`etl_fact_orders\` contains order identifiers, timestamps, keys, quantities, prices, statuses, derived revenue/margin and SLA flag. \`etl_dim_customer\` contains customer key, masked email/phone and loyalty attributes. Product and store dimensions contain catalogue and location attributes. Raw source columns should be treated as sensitive where applicable.
3. **Lineage:** \`etl_daily_zone_revenue\` is derived from validated order rows joined to store zone, then grouped by order date and zone. ETL makes rejected rows explicit through the issues report; ELT retains the original row in raw tables and links it to staging status.
4. **Personal data:** Customer name, email, phone, and potentially address/area/pincode are personal data. Reporting email/phone are masked rather than removed to support limited matching and customer-level analysis. India's Digital Personal Data Protection Act, 2023 and applicable rules should be assessed for lawful purpose, notice, safeguards, retention and data-principal rights.
5. **Access:** Raw customer PII should be limited to authorised CRM/engineering operators; staging access is restricted; business users receive masked marts. Quarantine contains source values and needs similarly restricted access.
6. **Retention:** Propose a documented, purpose-based retention schedule approved by Legal/Finance; delete or irreversibly anonymise after expiry, except records subject to legal holds.
7. **Secrets:** Use Airflow Connections/Variables or a managed secret backend. Never commit credentials, webhook URLs, or access tokens.
8. **Audit:** Find the order in raw source, inspect its staging/ETL failure reason, and confirm the rejected key is absent from finance fact/mart queries.
9. **Change history:** The increment should update the current status to RETURNED without duplicate order keys. Finance should use current net revenue for restated reporting and preserve an immutable raw/audit history to explain previous snapshots.

## Part F — Observability experiment log
**Evidence status:** Only mark an experiment complete when the described behavior was actually checked. Counts/results below were observed in the local DuckDB warehouse; screenshots/log evidence and the other experiments remain outstanding.

| # | Experiment | Expected | Observed / evidence |
|---|---|---|---|
| 1 | Happy path | DAG green; counts recorded | **Partially verified:** ELT DAG ran green. Recorded counts: `raw_orders` 21,708; `raw_orders_increment` 1,440; `stg_orders` 2,550. ETL happy-path evidence/screenshots still need to be attached. |
| 2 | Idempotency | Repeated identical ingestion does not increase raw counts; staging order keys remain unique | **ELT verified:** after a second green run, `raw_orders` stayed at 21,708 and `raw_orders_increment` at 1,440. `stg_orders` had 2,550 rows and 2,550 distinct order IDs. `stg_orders` DQ: 2,520 PASS, 30 FAIL. ETL fact idempotency not yet rechecked; capture screenshots/log evidence. |
| 3 | Incremental load | Confirm the expected 150 new IDs and 10 corrected IDs, with no duplicate current order keys | **Partial check only:** `raw_orders_increment` has 1,440 rows and 160 distinct order IDs; `stg_orders` has 2,550 rows and 2,550 distinct order IDs. The expected 150-new/10-corrected behavior has not yet been confirmed. |
| 4 | Missing file | Retries then failure; clear file-specific error | Pending local run |
| 5 | Five invalid order rows | Critical rows quarantined; DQ scorecard reflects failures | Pending local run |
| 6 | Schema change | Pipeline fails visibly on missing required column | **Implemented, not yet experimentally verified:** ELT ingestion now checks required columns for each source CSV and raises a file-specific schema error. Run the missing-column experiment locally and capture the Airflow failure log. |
| 7 | Volume anomaly | Warn if source volume falls >90% | **Implemented, local experiment pending:** ELT records per-file source row counts in `observability_metrics` and warns on a >90% drop versus the prior recorded run. Validate by safely testing with a temporary reduced copy and capture report evidence. |
| 8 | Distribution drift | Investigate store-level price/revenue shift | **Implemented, local experiment pending:** ELT records per-store average order amount and warns when it shifts by more than 50% versus the prior recorded run. Run a controlled test and capture report evidence. |
| 9 | Stale data | Timeliness warning | **Happy-path report checked; controlled stale-data experiment pending:** latest report showed `freshness` PASS at 0.15 hours (about 9 minutes). `future_timestamps` WARN showed 5 rows. Freshness now uses the newest non-future order timestamp, while future timestamps are flagged separately. A deliberately stale-data test and screenshot are still needed. |
| 10 | Recovery | Clear failed task; downstream tasks rerun | Pending local run |

**Five observability pillars:** freshness (happy-path report checked: 0.15 hours; 5 future timestamps flagged; controlled stale-data experiment pending), volume (per-file row counts recorded and all six current source-volume checks passed; >90% drop experiment pending), schema (required-column assertions added to ELT ingestion; experiment still needs local verification), distribution (per-store averages recorded and all listed current store checks passed; >50% drift experiment pending), lineage (raw ingestion metadata includes source file and run ID; evidence capture pending).

## Part G — Recommendation to Sameer (CTO)
**Recommendation: ELT with a strict governed mart boundary, plus ETL-style validation for finance-critical outputs.** NammaMart needs raw records preserved exactly for audit, replay and late status corrections. ELT makes it easier to change rules in SQL and compare historical versions without repeatedly rebuilding source extracts. As data volume grows, a warehouse can optimise SQL and incremental transformations, while open-source DuckDB is cost-effective for this assignment's local scale. However, raw retention must never mean raw PII is broadly accessible: restrict raw tables, mask staging/marts, and quarantine bad records. Finance should consume only validated marts and reconciliation exceptions, never raw tables.
**Top risks:** (1) a flawed SQL rule can contaminate marts if quality gates are bypassed; (2) unrestricted raw data creates privacy risk; (3) a laptop-hosted warehouse does not provide production availability, alerting or concurrent-user capacity. A production deployment should add CI tests, alerting, access controls, backups, and a managed/continuously available runtime.

## Business questions — SQL starter queries
\`\`\`sql
-- 1. Daily revenue by zone/store
SELECT order_date, zone, store_id, store_name, SUM(net_revenue) revenue
FROM etl_fact_orders GROUP BY ALL ORDER BY 1,2,3;

-- 2. Revenue and margin by category
SELECT category, SUM(net_revenue) revenue, SUM(gross_margin) gross_margin
FROM etl_fact_orders GROUP BY category ORDER BY revenue DESC;

-- 3. Average delivery and <=15-minute share
SELECT store_id, store_name, AVG(try_cast(delivery_minutes AS DOUBLE)) avg_minutes,
       AVG(CASE WHEN within_15_min THEN 1.0 ELSE 0 END)*100 pct_within_15
FROM etl_fact_orders WHERE order_status='DELIVERED' GROUP BY ALL;

-- 4. Payment mode and reconciliation exceptions
SELECT payment_mode, SUM(net_revenue) revenue FROM etl_fact_orders GROUP BY payment_mode;
SELECT * FROM etl_payment_reconciliation;

-- 5. Top customers (masked contact details are not selected)
SELECT customer_id, loyalty_tier, SUM(net_revenue) spend
FROM etl_fact_orders GROUP BY ALL ORDER BY spend DESC LIMIT 10;

-- 6. Rejections by file and reason
SELECT file, problem, COUNT(*) issue_count
FROM read_csv_auto('include/reports/data_issues_*.csv', union_by_name=true)
GROUP BY ALL ORDER BY issue_count DESC;
\`\`\`

## Before submission
- [ ] Install/start Docker Desktop and Astro CLI on Windows.
- [ ] Copy the six CSVs to \`include/data/nammamart/\`.
- [ ] Add \`pandas\`, \`duckdb\` to \`requirements.txt\`; restart Astro.
- [ ] Run \`astro dev run dags list-import-errors\`, then trigger both DAGs.
- [ ] Fix any import/runtime issues and verify DuckDB table counts and PII masking.
- [ ] Run all ten experiments, record actual observations, and capture required Airflow screenshots.
