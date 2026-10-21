# NammaMart ETL & ELT Data Pipeline — Submission

**Author:** Surbhi Soni  
**Working branch:** `namamart-assignment-work`  
**Warehouse:** `include/warehouse_surbhi.duckdb`  
**DAGs:** `nammamart_etl_surbhi`, `nammamart_elt_surbhi`

## 1. Solution overview

This submission implements two comparable pipelines over the same six source exports.

- **ETL:** parallel source-file/schema checks, Python validation, quarantine reports, PII masking, a DuckDB fact/dimension model, revenue/margin summaries, and payment reconciliation.
- **ELT:** raw source tables with ingestion metadata, SQL transformations stored in `include/sql/elt_staging_dimensions.sql`, `include/sql/elt_staging_orders.sql`, and `include/sql/elt_marts.sql`, newest-version-wins order staging for the incremental feed, and source-volume/freshness/distribution reports.
- **Schedule:** `0 9 * * 1-6`, explicitly in `Asia/Kolkata`; 09:00 IST equals 03:30 UTC. `catchup=False` avoids replaying historical intervals. A laptop-based scheduler cannot run while Docker/the host is off, so a production deployment needs an always-on runtime and missed-run alerts.
- **Auditability:** `dq_results` stores per-rule results across the six dimensions; `pipeline_runs` stores run summaries. Both DAGs include an `all_done` final audit task to cover failures that happen before the main pipeline helper starts.

Source CSVs are inputs and must not be manually edited. Experiments must use temporary copies and restore the originals immediately afterward.

## Part C. Scheduling and orchestration answers

- **Schedule:** `0 9 * * 1-6` in `Asia/Kolkata` means 09:00 IST, Monday through Saturday. It does not schedule a Sunday run.
- **Catch-up:** `catchup=False` prevents Airflow from automatically creating runs for all missed historical intervals when the DAG is first enabled.
- **Retries:** each DAG allows two retries with a three-minute delay. This helps with transient failures; it will not repair bad source data or a persistent schema error.
- **Dependencies:** the six source/schema-check tasks feed the main transform/load task. A final audit task uses `all_done` so it can attempt to record an audit outcome after the main task finishes.
- **Operational limitation:** the local Astro/Docker scheduler can run only while the host and Docker environment are available. Production scheduling needs an always-on orchestrator, alerting, and a documented recovery process.

## Part 0. Initial profiling and issue log

The profiler writes a per-run column-level summary and flags nulls and duplicate business keys across the five base files and the incremental order feed. The following issues have been observed in the available run evidence; they are data-quality findings, not proof that the source file was changed.

| Issue | Evidence / impact | Handling |
|---|---|---|
| Five future-dated order timestamps | The ELT observability report identified five future timestamps; sample rows are listed in `include/reports/future_order_timestamps.csv`. Future dates can distort freshness and time-based metrics. | Keep source unchanged, flag the records, and review with the data owner before correcting source values. |
| Incremental feed contains order versions | Base orders and `orders_2026-10-01_increment.csv` are both ingested. The latest version should win for the current-state staging/fact model, while raw ELT inputs remain available for audit. | Compare order IDs and changed fields; verify idempotency and correction behavior with a controlled experiment. |
| Source-level nulls, duplicates, invalid values, and schema drift | The profiler and per-rule scorecards are designed to record these; actual counts vary by run and must be read from that run's report and `dq_results`. | Quarantine rejected rows where supported; fail clearly on missing required columns; do not claim a test passed without run evidence. |
| Reconciliation exceptions | Payment/order amount mismatches, missing payments, and unsuccessful payment statuses are represented in reconciliation tables. | Review exceptions with Finance; do not silently force payment totals to match. |

**Open questions for the business:** confirm the policy for negative quantities/returns, whether late return status changes restate the original order date or book a reversal on the return date, and the matching tolerance/allocation policy for partial or split payments.

## 2. Profiling and issue handling

The shared profiler inspects all six source files for nulls and duplicate business keys. ETL checks order status, timestamp, discount range, quantity, calculated amount, master-key integrity, delivered-order duration, and future timestamps. Schema assertions fail loudly when required columns are absent. The ELT scorecard also checks completeness, uniqueness, validity, accuracy, consistency, and timeliness with separate rows per rule.

The per-run column-level profile (row counts, nulls, distinct counts, numeric/date ranges, and selected categorical distributions) is written to `include/reports/profile_summary_<run_id>.csv`; issue and observability artifacts are also written under `include/reports/`. Rejected ETL orders are written under `include/quarantine/surbhi/`. Reports should be reviewed by source file, row key, column, quality dimension, and severity before a business rule is changed.

### Questions for the client

1. Should a negative quantity mean a return, or should it always be treated as a data-entry error?
2. When an order changes to RETURNED in a later feed, should Finance restate the original order date or book the reversal on the return date?
3. What matching tolerance and allocation rules should apply to partial or split payments?

## 3. Data quality scorecard

The scorecard writes the required fields: `run_id`, `dag_id`, `check_name`, `dimension`, `severity`, `rows_checked`, `rows_failed`, `status`, and `checked_at`.

| Dimension | ETL checks | ELT checks |
|---|---|---|
| Completeness | Required fields in orders, payments, customers, products, stores | SQL required-field checks against raw tables |
| Uniqueness | Order, payment, customer, product, and store business keys | SQL duplicate-key checks against raw tables |
| Validity | Allowed order/payment values and email format | SQL allowed-value checks |
| Accuracy | Order amount formula and positive product prices | SQL amount formula and product-price checks |
| Consistency | Order-to-customer/product/store keys and payment-to-order keys | SQL referential-integrity checks |
| Timeliness | Future and stale order timestamps | SQL future and stale timestamp checks |

Critical failures are recorded as `FAIL`; warning-level failures are recorded as `WARN`. A scorecard row is emitted for each rule on each run.

## 4. Governance and data dictionary

### Ownership and rule approval
Operations owns orders and store master data; Finance owns payment records and reconciliation policy; CRM owns customer data; Merchandising owns product data; Data Engineering owns warehouse models and pipeline controls. Changes to financial rules require the relevant business owner and Finance approval, with a versioned change record.

### Core warehouse dictionary

| Table | Important columns | Meaning / source | PII |
|---|---|---|---|
| `etl_fact_orders` | `order_id`, `order_ts`, `customer_id`, `product_id`, `store_id`, `quantity`, `order_amount`, `net_revenue`, `gross_margin`, `within_15_min` | Validated order rows joined to CRM, product, and store dimensions | Customer key may be linkable; email/phone are masked |
| `etl_dim_customer` | `customer_id`, `email`, `phone`, `city`, `area`, `pincode`, `loyalty_tier` | CRM customer attributes | Yes; email/phone masked in reporting table |
| `etl_dim_product` | `product_id`, `category`, `mrp`, `cost_price` | Product catalogue and pricing | No |
| `etl_dim_store` | `store_id`, `zone`, `area`, `capacity_orders_per_day` | Store master | Manager name may identify a person |
| `raw_orders` / `raw_payments` | Source columns plus `_loaded_at`, `_source_file`, `_run_id` | Original ELT inputs for replay and audit | Depends on source fields |
| `stg_orders` | Typed columns, `dq_status`, `failure_reason` | Current order version after SQL standardisation and checks | Customer key may be linkable |
| `mart_daily_store_kpis` | `order_date`, `zone`, `store_id`, `net_revenue` | Daily delivered-order revenue by store | No direct contact details |
| `mart_category_margin` | `category`, `revenue`, `gross_margin` | Revenue and margin by category | No |
| `mart_delivery_sla` | `avg_delivery_minutes`, `pct_within_15_min` | Store delivery performance | No |
| `mart_payment_reconciliation` | `order_id`, `order_amount`, `amount_paid`, `failure_reason` | Payment/order exceptions | Order identifier only |
| `mart_top_customers` | `customer_id`, `loyalty_tier`, `spend` | Top customers by delivered spend; no contact details | Customer key is linkable |

### Lineage, privacy, access, retention, and secrets
A store's daily revenue starts with an order row in `orders.csv`, passes ETL order validation or ELT staging rules, joins to `stores.csv` for the zone/store attributes, and is grouped by order date and store in the corresponding daily mart. ELT raw tables retain source-file and run metadata; rejected orders can be traced to their staging failure reason. ETL's issue/quarantine files identify rejected order keys and reasons.

Customer name, email, phone, and address-like attributes (city, area, pincode) can be personal data. Keep unmasked source PII restricted to authorised operators; provide masked contact fields or aggregated marts to business users. Apply the Indian Digital Personal Data Protection Act, 2023 and applicable rules as confirmed by counsel. Retention should be purpose-based, approved by Legal/Finance, and followed by deletion or irreversible anonymisation unless a legal hold applies. Store credentials only in Airflow Connections/Variables or a secret backend; never commit them to GitHub.

## 5. Observability and run evidence

The pipeline emits source-volume metrics, a freshness age based on the newest non-future order timestamp, a separate future-timestamp count, and per-store average order-amount drift metrics. Schema checks fail on missing required columns. The ELT raw layer records source file and run identifiers. Query the following after each run:

```sql
SELECT * FROM dq_results
ORDER BY checked_at DESC, dag_id, dimension, check_name;

SELECT * FROM pipeline_runs
ORDER BY end_time DESC;

SELECT * FROM etl_payment_reconciliation
ORDER BY order_id;

SELECT * FROM mart_payment_reconciliation
ORDER BY order_id;
```

### Observed local ELT run
- DAG run completed green in the local Airflow environment.
- `raw_orders`: 21,708 rows; `raw_orders_increment`: 1,440 rows; `stg_orders`: 2,550 rows.
- On the repeated run, raw table counts remained stable and staging contained 2,550 rows with 2,550 distinct order IDs.
- A happy-path observability report recorded freshness at approximately 0.17 hours and flagged five future-dated source order timestamps. The original source CSV was not modified; the identified rows were exported separately to `include/reports/future_order_timestamps.csv`.

### Ten required experiments

Use the Airflow Graph/Log views and DuckDB tables to record the actual run ID, expected result, observed result, evidence location, and follow-up for each experiment. Do not describe an experiment as passed until the controlled run has been performed.

| # | Experiment / check | Implementation / available evidence | Status |
|---|---|---|---|
| 1 | Happy path | Successful local ELT run and green DAG screenshot | [x] Evidence available |
| 2 | Idempotency | Repeated ELT run; raw counts stable and staging has 2,550 distinct order IDs | [x] Evidence available |
| 3 | Incremental feed | Incremental source is ingested and newest-version-wins staging logic is implemented | [x] Implementation documented; controlled correction scenario not separately recorded |
| 4 | Missing `products.csv` | Required-source/schema validation and final audit handling are implemented | [x] Implementation documented; fault injection not separately recorded |
| 5 | Five invalid orders | Quarantine and failure-reason reporting are implemented; use screenshot 04 and the generated quarantine CSV as evidence if the reason column is populated | [x] Implementation documented; verify artifact contents before claiming a specific run passed |
| 6 | Schema change | Required-column assertions and failure logging are implemented | [x] Implementation documented; controlled schema-change run not separately recorded |
| 7 | Volume anomaly | Source-volume observability and >90% drop detection are implemented | [x] Implementation documented; controlled anomaly run not separately recorded |
| 8 | Distribution drift | Per-store distribution-drift monitoring is implemented | [x] Implementation documented; controlled drift run not separately recorded |
| 9 | Stale feed | Freshness monitoring and stale-feed warning logic are implemented | [x] Implementation documented; controlled stale-feed run not separately recorded |
| 10 | Recovery | Retries and final audit task are configured | [x] Implementation documented; controlled recovery run not separately recorded |

**Five observability pillars:** freshness (timestamp age and future-date check); volume (per-file counts and >90% drop detection); schema (required-column assertions); distribution (per-store average order amount); lineage (source file and run metadata). Lineage is strongest for ELT raw ingestion; row-level lineage from aggregated revenue back to exact source records should be improved for production use.

## 6. Business questions

The paired ETL and ELT SQL for all six required business questions is in `include/sql/business_questions.sql`. It covers daily store/zone revenue, category revenue and margin, delivery SLA, payment-mode revenue and reconciliation, top customers without contact details, and rejection counts/reasons. Run both versions and record their result sets to demonstrate that the September totals agree; document any intentional difference caused by incremental status corrections.

## 7. Recommendation to Sameer, CTO

**Recommendation: adopt ELT for the governed analytical platform, with a strict validated-mart boundary for Finance.** Preserving raw inputs makes audits, replays, and late corrections easier; SQL transformations are easier to evolve as business rules change. Finance should consume only validated marts and reconciliation exceptions, never raw tables. For this local assignment, DuckDB keeps infrastructure cost low; at 1,000× scale, move to a managed warehouse/orchestrator with partitioning, incremental models, access controls, backups, and alerting.

Top risks are (1) an incorrect SQL rule contaminating a mart if quality gates are bypassed, (2) privacy exposure if raw customer data is too broadly accessible, and (3) local-host availability and concurrency limits. Mitigations are mandatory scorecard gates, least-privilege access, retention policy, automated tests, run alerts, and a continuously available production runtime.

## 8. Submission evidence checklist

- [ ] Both DAGs parse without import errors and run successfully.
- [ ] Screenshot of both DAG Graph views in green state.
- [ ] Screenshot showing 09:00 IST schedule and next-run time.
- [ ] Example quarantine CSV with a `failure_reason`.
- [ ] Query results for `dq_results`, `pipeline_runs`, and both payment reconciliation tables.
- [ ] Implementation and evidence review checklist completed. Controlled fault-injection scenarios not separately recorded are identified as implementation evidence, not claimed as executed tests.
- [ ] Six business-question SQL queries run for both pipelines and results compared.
- [ ] Confirm no secrets or unmasked customer PII were committed outside the source-data directory.
