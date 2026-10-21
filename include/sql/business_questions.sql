-- NammaMart business questions (ETL tables)
-- 1. Daily net revenue by zone and store
SELECT order_date, zone, store_id, store_name, SUM(net_revenue) AS net_revenue
FROM etl_fact_orders GROUP BY ALL ORDER BY order_date, zone, store_id;

-- 2. Revenue and gross margin by category
SELECT category, SUM(net_revenue) AS revenue, SUM(gross_margin) AS gross_margin
FROM etl_fact_orders GROUP BY category ORDER BY revenue DESC;

-- 3. Delivery time and 15-minute service level by store
SELECT store_id, store_name, AVG(TRY_CAST(delivery_minutes AS DOUBLE)) AS avg_delivery_minutes,
       AVG(CASE WHEN within_15_min THEN 1.0 ELSE 0.0 END) * 100 AS pct_within_15_min
FROM etl_fact_orders WHERE order_status = 'DELIVERED' GROUP BY ALL;

-- 4. Revenue by payment mode
SELECT payment_mode, SUM(net_revenue) AS revenue
FROM etl_fact_orders GROUP BY payment_mode ORDER BY revenue DESC;
SELECT * FROM etl_payment_reconciliation ORDER BY order_id;

-- 5. Top 10 customers by spend. Contact information is intentionally excluded.
SELECT customer_id, loyalty_tier, SUM(net_revenue) AS spend
FROM etl_fact_orders WHERE order_status = 'DELIVERED'
GROUP BY customer_id, loyalty_tier ORDER BY spend DESC LIMIT 10;

-- 6. Data issues by source file and reason. Run from project root.
SELECT file, problem, COUNT(*) AS issue_count
FROM read_csv_auto('include/reports/data_issues_*.csv', union_by_name = true)
GROUP BY file, problem ORDER BY issue_count DESC;
