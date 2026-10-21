CREATE OR REPLACE TABLE stg_customers AS SELECT DISTINCT customer_id,
          CASE WHEN strpos(email,'@')>0 THEN substr(email,1,2)||'***@'||split_part(email,'@',2) ELSE '***' END email_masked,
          CASE WHEN length(regexp_replace(phone,'[^0-9]','','g'))>=4 THEN substr(regexp_replace(phone,'[^0-9]','','g'),1,2)||'******'||right(regexp_replace(phone,'[^0-9]','','g'),2) ELSE '***' END phone_masked,
          city,area,pincode,signup_date,upper(trim(loyalty_tier)) loyalty_tier,is_active FROM raw_customers;

CREATE OR REPLACE TABLE stg_products AS SELECT DISTINCT product_id,trim(product_name) product_name,trim(category) category,sub_category,brand,
          try_cast(mrp AS DOUBLE) mrp,try_cast(cost_price AS DOUBLE) cost_price,unit,is_perishable,launch_date FROM raw_products;

CREATE OR REPLACE TABLE stg_stores AS SELECT DISTINCT store_id,trim(store_name) store_name,upper(trim("zone")) AS zone,area,pincode,open_date,manager_name,try_cast(capacity_orders_per_day AS INTEGER) capacity_orders_per_day FROM raw_stores;

CREATE OR REPLACE TABLE stg_payments AS SELECT payment_id,order_id,payment_ts,trim(payment_mode) payment_mode,try_cast(amount_paid AS DOUBLE) amount_paid,upper(trim(payment_status)) payment_status,gateway_ref FROM raw_payments;
