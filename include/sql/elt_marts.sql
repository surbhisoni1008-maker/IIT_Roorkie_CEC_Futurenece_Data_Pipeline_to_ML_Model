-- Reference SQL for ELT marts; the current DAG also creates these tables in its SQL task.
-- Daily revenue by zone/store
CREATE OR REPLACE TABLE mart_daily_store_kpis AS
SELECT CAST(o.order_ts_cast AS DATE) AS order_date, s.zone, s.store_id, s.store_name,
       SUM(CASE WHEN o.order_status_std='DELIVERED' THEN o.order_amount_cast ELSE 0 END) AS net_revenue,
       COUNT(*) FILTER (WHERE o.order_status_std='DELIVERED') AS delivered_orders
FROM stg_orders o LEFT JOIN stg_stores s USING (store_id)
WHERE o.dq_status='PASS' GROUP BY ALL;

-- Revenue and gross margin by category
CREATE OR REPLACE TABLE mart_category_margin AS
SELECT p.category,
       SUM(CASE WHEN o.order_status_std='DELIVERED' THEN o.order_amount_cast ELSE 0 END) AS revenue,
       SUM(CASE WHEN o.order_status_std='DELIVERED' THEN o.order_amount_cast ELSE 0 END)
       - SUM(CASE WHEN o.order_status_std='DELIVERED' THEN o.quantity_cast*p.cost_price ELSE 0 END) AS gross_margin
FROM stg_orders o LEFT JOIN stg_products p USING (product_id)
WHERE o.dq_status='PASS' GROUP BY p.category;
