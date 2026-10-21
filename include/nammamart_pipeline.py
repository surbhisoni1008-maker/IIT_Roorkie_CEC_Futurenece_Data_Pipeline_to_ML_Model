"""Shared NammaMart pipeline utilities. Never edit source CSV files."""
from __future__ import annotations
import csv, json, re
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd
import duckdb

ROOT = Path(__file__).resolve().parents[1]
DATA_CANDIDATES = [ROOT / "include" / "data" / "nammamart", ROOT / "assignment" / "nammamart_dataset"]
FILES = {
    "orders": "orders.csv", "payments": "payments.csv", "customers": "customers.csv",
    "products": "products.csv", "stores": "stores.csv"
}
INCREMENT = "orders_2026-10-01_increment.csv"
WAREHOUSE = ROOT / "include" / "warehouse_surbhi.duckdb"
def data_dir():
    for p in DATA_CANDIDATES:
        if p.exists() and (p / "orders.csv").exists(): return p
    raise FileNotFoundError("Dataset not found. Copy assignment/nammamart_dataset into include/data/nammamart.")
def connect():
    WAREHOUSE.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(WAREHOUSE))
def read_csv(name, dtype=None):
    return pd.read_csv(data_dir() / name, dtype=dtype, keep_default_na=True)
def mask_email(v):
    if pd.isna(v) or not str(v).strip(): return None
    s=str(v).strip()
    if "@" not in s: return "***"
    left, right=s.split("@",1)
    return (left[:2] + "***@" + right) if len(left)>2 else (left[:1]+"***@"+right)
def mask_phone(v):
    if pd.isna(v): return None
    digits=re.sub(r"\D","",str(v))
    return (digits[:2] + "*"*max(0,len(digits)-4) + digits[-2:]) if len(digits)>=4 else "***"
def ensure_dirs():
    for p in [ROOT/"include"/"quarantine"/"surbhi", ROOT/"include"/"reports", ROOT/"include"/"sql"]:
        p.mkdir(parents=True, exist_ok=True)
def dq_orders(df, customers, products, stores, run_id):
    """Return row-level issues. Critical failures are quarantined."""
    checks=[]
    def add(mask, col, problem, dimension, severity="Critical"):
        for idx in df.index[mask.fillna(False)]:
            checks.append({"file":"orders.csv","row_key":str(df.at[idx,"order_id"]) if "order_id" in df else str(idx),
              "column":col,"problem":problem,"quality_dimension":dimension,"severity":severity,
              "run_id":run_id})
    add(df["order_id"].isna(), "order_id", "Missing order identifier", "Completeness")
    add(df["customer_id"].isna(), "customer_id", "Missing customer key", "Completeness")
    add(df["quantity"].isna() | (pd.to_numeric(df["quantity"],errors="coerce")<=0), "quantity", "Quantity must be positive", "Accuracy")
    add(~df["order_status"].astype(str).str.strip().str.upper().isin(["DELIVERED","CANCELLED","RETURNED"]), "order_status", "Unknown order status", "Validity")
    add(pd.to_datetime(df["order_ts"],errors="coerce").isna(), "order_ts", "Invalid timestamp", "Validity")
    add(pd.to_numeric(df["discount_pct"],errors="coerce").isna() | ~pd.to_numeric(df["discount_pct"],errors="coerce").between(0,100), "discount_pct", "Discount must be between 0 and 100", "Validity")
    calc=(pd.to_numeric(df["quantity"],errors="coerce")*pd.to_numeric(df["unit_price"],errors="coerce")*(1-pd.to_numeric(df["discount_pct"],errors="coerce")/100)).round(2)
    amt=pd.to_numeric(df["order_amount"],errors="coerce")
    add((calc-amt).abs()>0.02, "order_amount", "Amount does not match quantity × unit price × discount", "Accuracy")
    add(df["order_id"].duplicated(keep="first"), "order_id", "Duplicate order identifier", "Uniqueness")
    add(~df["customer_id"].isin(customers["customer_id"]), "customer_id", "Customer key not found in customer master", "Consistency")
    add(~df["product_id"].isin(products["product_id"]), "product_id", "Product key not found in product master", "Consistency")
    add(~df["store_id"].isin(stores["store_id"]), "store_id", "Store key not found in store master", "Consistency")
    delivered=df["order_status"].astype(str).str.strip().str.upper().eq("DELIVERED")
    add(delivered & pd.to_numeric(df["delivery_minutes"],errors="coerce").isna(), "delivery_minutes", "Delivered order has no delivery duration", "Completeness")
    add(delivered & (pd.to_numeric(df["delivery_minutes"],errors="coerce")>=120), "delivery_minutes", "Delivery duration is implausibly high (>=120 minutes)", "Accuracy")
    ts=pd.to_datetime(df["order_ts"],errors="coerce")
    add(ts > pd.Timestamp.now(tz=None), "order_ts", "Order timestamp is in the future", "Timeliness")
    issues=pd.DataFrame(checks)
    failed_keys=set(issues.loc[issues.severity.eq("Critical"),"row_key"].astype(str)) if not issues.empty else set()
    failed=df["order_id"].astype(str).isin(failed_keys)
    return df.loc[~failed].copy(), df.loc[failed].copy(), issues
def profile_all(run_id):
    ensure_dirs()
    d=data_dir()
    records=[]
    for fn in [*FILES.values(), INCREMENT]:
        frame=pd.read_csv(d/fn)
        for col in frame.columns:
            nulls=int(frame[col].isna().sum())
            if nulls: records.append({"file":fn,"row_key":"*","column":col,"problem":f"{nulls} null values","quality_dimension":"Completeness","severity":"Warning","run_id":run_id})
        key = "order_id" if "order_id" in frame else ("payment_id" if "payment_id" in frame else ("customer_id" if "customer_id" in frame else ("product_id" if "product_id" in frame else ("store_id" if "store_id" in frame else None))))
        if key and frame[key].duplicated().any():
            records.append({"file":fn,"row_key":"*","column":key,"problem":f"{int(frame[key].duplicated().sum())} duplicate keys","quality_dimension":"Uniqueness","severity":"Critical","run_id":run_id})
    return pd.DataFrame(records)
def record_dq(con, run_id, dag_id, issues, rows_checked):
    con.execute("""CREATE TABLE IF NOT EXISTS dq_results(run_id VARCHAR,dag_id VARCHAR,check_name VARCHAR,dimension VARCHAR,severity VARCHAR,rows_checked BIGINT,rows_failed BIGINT,status VARCHAR,checked_at TIMESTAMP)""")
    now=datetime.now(timezone.utc).replace(tzinfo=None)
    if issues.empty:
        con.execute("INSERT INTO dq_results VALUES (?,?,?,?,?,?,?,?,?)",[run_id,dag_id,"orders_critical_rules","Multiple","Critical",rows_checked,0,"PASS",now])
    else:
        for dim, group in issues.groupby("quality_dimension"):
            con.execute("INSERT INTO dq_results VALUES (?,?,?,?,?,?,?,?,?)",[run_id,dag_id,f"{dim.lower()}_checks",dim,group["severity"].iloc[0],rows_checked,len(group),"FAIL" if group["severity"].eq("Critical").any() else "WARN",now])
def record_run(con, run_id, dag_id, start, extracted, loaded, quarantined, status):
    con.execute("""CREATE TABLE IF NOT EXISTS pipeline_runs(run_id VARCHAR,dag_id VARCHAR,start_time TIMESTAMP,end_time TIMESTAMP,rows_extracted BIGINT,rows_loaded BIGINT,rows_quarantined BIGINT,status VARCHAR)""")
    con.execute("INSERT INTO pipeline_runs VALUES (?,?,?,?,?,?,?,?)",[run_id,dag_id,start,datetime.now(timezone.utc).replace(tzinfo=None),extracted,loaded,quarantined,status])
def run_etl(run_id):
    ensure_dirs(); start=datetime.now(timezone.utc).replace(tzinfo=None); con=connect()
    try:
        orders=read_csv(FILES["orders"]); payments=read_csv(FILES["payments"])
        customers=read_csv(FILES["customers"]).drop_duplicates("customer_id")
        products=read_csv(FILES["products"]).drop_duplicates("product_id")
        stores=read_csv(FILES["stores"]).drop_duplicates("store_id")
        issues=profile_all(run_id)
        clean,bad,order_issues=dq_orders(orders,customers,products,stores,run_id)
        issues=pd.concat([issues,order_issues],ignore_index=True)
        if not issues.empty: issues.to_csv(ROOT/"include"/"reports"/f"data_issues_{run_id.replace(':','_')}.csv",index=False)
        if not bad.empty:
            bad=bad.copy(); bad["failure_reason"]="See data_issues report for order_id"
            bad.to_csv(ROOT/"include"/"quarantine"/"surbhi"/f"orders_failed_{run_id.replace(':','_')}.csv",index=False)
        for name, frame in [("etl_dim_customer",customers),("etl_dim_product",products),("etl_dim_store",stores)]:
            frame=frame.copy()
            if name=="etl_dim_customer":
                frame["email"]=frame["email"].map(mask_email); frame["phone"]=frame["phone"].map(mask_phone)
            con.register("tmp_frame",frame); con.execute(f"CREATE OR REPLACE TABLE {name} AS SELECT * FROM tmp_frame"); con.unregister("tmp_frame")
        clean["order_status"]=clean["order_status"].astype(str).str.strip().str.upper()
        clean["payment_mode"]=clean["payment_mode"].astype(str).str.strip().str.upper().replace({"CASH ON DELIVERY":"Cash on Delivery"})
        clean["order_date"]=pd.to_datetime(clean["order_ts"],errors="coerce").dt.date
        clean["net_revenue"]=pd.to_numeric(clean["order_amount"],errors="coerce").where(clean["order_status"].eq("DELIVERED"),0)
        clean=clean.merge(products[["product_id","category","cost_price"]],on="product_id",how="left")
        clean["gross_margin"]=clean["net_revenue"]-pd.to_numeric(clean["quantity"],errors="coerce")*pd.to_numeric(clean["cost_price"],errors="coerce")
        clean["within_15_min"]=pd.to_numeric(clean["delivery_minutes"],errors="coerce").le(15)
        clean=clean.merge(stores[["store_id","store_name","zone"]],on="store_id",how="left")
        clean=clean.merge(customers[["customer_id","loyalty_tier","email","phone"]].assign(email=lambda x:x.email.map(mask_email),phone=lambda x:x.phone.map(mask_phone)),on="customer_id",how="left",suffixes=("","_masked"))
        con.register("tmp_frame",clean); con.execute("CREATE OR REPLACE TABLE etl_fact_orders AS SELECT * FROM tmp_frame"); con.unregister("tmp_frame")
        con.execute("CREATE OR REPLACE TABLE etl_daily_zone_revenue AS SELECT order_date,zone,store_id,store_name,SUM(net_revenue) revenue FROM etl_fact_orders GROUP BY ALL")
        con.execute("CREATE OR REPLACE TABLE etl_category_margin AS SELECT category,SUM(net_revenue) revenue,SUM(gross_margin) gross_margin FROM etl_fact_orders GROUP BY category")
        con.execute("""CREATE OR REPLACE TABLE etl_payment_reconciliation AS
            SELECT o.order_id,o.order_amount,p.amount_paid,p.payment_status,
            CASE WHEN p.order_id IS NULL THEN 'No payment found'
                 WHEN abs(try_cast(o.order_amount AS DOUBLE)-try_cast(p.amount_paid AS DOUBLE))>0.02 THEN 'Amount mismatch'
                 WHEN upper(p.payment_status)!='SUCCESS' THEN 'Payment not successful' END failure_reason
            FROM etl_fact_orders o LEFT JOIN payments p USING(order_id)
            WHERE p.order_id IS NULL OR abs(try_cast(o.order_amount AS DOUBLE)-try_cast(p.amount_paid AS DOUBLE))>0.02 OR upper(p.payment_status)!='SUCCESS'""")
        record_dq(con,run_id,"nammamart_etl_surbhi",issues,len(orders))
        record_run(con,run_id,"nammamart_etl_surbhi",start,len(orders)+len(payments)+len(customers)+len(products)+len(stores),len(clean),len(bad),"success")
    except Exception:
        record_run(con,run_id,"nammamart_etl_surbhi",start,0,0,0,"failed")
        raise
    finally: con.close()
def run_elt(run_id):
    ensure_dirs(); start=datetime.now(timezone.utc).replace(tzinfo=None); con=connect(); extracted=loaded=0
    try:
        d=data_dir()
        for logical, fn in FILES.items():
            raw=pd.read_csv(d/fn,dtype=str,keep_default_na=False)
            raw["_loaded_at"]=datetime.now(timezone.utc).isoformat(); raw["_source_file"]=fn; raw["_run_id"]=run_id
            extracted+=len(raw)
            con.register("raw_input",raw)
            table=f"raw_{logical}"
            con.execute(f"CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM raw_input WHERE 1=0")
            con.execute(f"INSERT INTO {table} BY NAME SELECT * FROM raw_input")
            con.unregister("raw_input")
        # Raw increment is append-only; typed staging is upserted on order_id.
        inc=d/INCREMENT
        if inc.exists():
            raw=pd.read_csv(inc,dtype=str,keep_default_na=False); raw["_loaded_at"]=datetime.now(timezone.utc).isoformat(); raw["_source_file"]=INCREMENT; raw["_run_id"]=run_id
            extracted+=len(raw); con.register("raw_input",raw)
            con.execute("CREATE TABLE IF NOT EXISTS raw_orders_increment AS SELECT * FROM raw_input WHERE 1=0")
            con.execute("INSERT INTO raw_orders_increment BY NAME SELECT * FROM raw_input"); con.unregister("raw_input")
        # SQL-only staging and marts. Raw data is retained even when staging rejects it.
        con.execute("""CREATE OR REPLACE TABLE stg_customers AS SELECT DISTINCT customer_id,full_name,
          CASE WHEN strpos(email,'@')>0 THEN substr(email,1,2)||'***@'||split_part(email,'@',2) ELSE '***' END email_masked,
          CASE WHEN length(regexp_replace(phone,'[^0-9]','','g'))>=4 THEN substr(regexp_replace(phone,'[^0-9]','','g'),1,2)||'******'||right(regexp_replace(phone,'[^0-9]','','g'),2) ELSE '***' END phone_masked,
          city,area,pincode,signup_date,upper(trim(loyalty_tier)) loyalty_tier,is_active FROM raw_customers""")
        con.execute("""CREATE OR REPLACE TABLE stg_products AS SELECT DISTINCT product_id,trim(product_name) product_name,trim(category) category,sub_category,brand,
          try_cast(mrp AS DOUBLE) mrp,try_cast(cost_price AS DOUBLE) cost_price,unit,is_perishable,launch_date FROM raw_products""")
        con.execute("""CREATE OR REPLACE TABLE stg_stores AS SELECT DISTINCT store_id,trim(store_name) store_name,upper(trim(zone)) zone,area,pincode,open_date,manager_name,try_cast(capacity_orders_per_day AS INTEGER) capacity_orders_per_day FROM raw_stores""")
        con.execute("""CREATE OR REPLACE TABLE stg_payments AS SELECT payment_id,order_id,payment_ts,trim(payment_mode) payment_mode,try_cast(amount_paid AS DOUBLE) amount_paid,upper(trim(payment_status)) payment_status,gateway_ref FROM raw_payments""")
        con.execute("""CREATE OR REPLACE TABLE stg_orders AS
          SELECT *,try_cast(order_ts AS TIMESTAMP) order_ts_cast,try_cast(quantity AS INTEGER) quantity_cast,
          try_cast(unit_price AS DOUBLE) unit_price_cast,try_cast(discount_pct AS DOUBLE) discount_pct_cast,
          try_cast(order_amount AS DOUBLE) order_amount_cast,try_cast(delivery_minutes AS DOUBLE) delivery_minutes_cast,
          upper(trim(order_status)) order_status_std,
          CASE WHEN order_id IS NULL OR trim(order_id)='' THEN 'Missing order_id'
            WHEN try_cast(order_ts AS TIMESTAMP) IS NULL THEN 'Invalid timestamp'
            WHEN try_cast(quantity AS INTEGER)<=0 THEN 'Quantity must be positive'
            WHEN upper(trim(order_status)) NOT IN ('DELIVERED','CANCELLED','RETURNED') THEN 'Invalid status'
            WHEN try_cast(discount_pct AS DOUBLE) NOT BETWEEN 0 AND 100 THEN 'Invalid discount'
            WHEN upper(trim(order_status))='DELIVERED' AND try_cast(delivery_minutes AS DOUBLE) IS NULL THEN 'Missing delivery time'
            ELSE NULL END failure_reason,
          CASE WHEN order_id IS NULL OR trim(order_id)='' OR try_cast(order_ts AS TIMESTAMP) IS NULL OR try_cast(quantity AS INTEGER)<=0
            OR upper(trim(order_status)) NOT IN ('DELIVERED','CANCELLED','RETURNED') OR try_cast(discount_pct AS DOUBLE) NOT BETWEEN 0 AND 100
            OR (upper(trim(order_status))='DELIVERED' AND try_cast(delivery_minutes AS DOUBLE) IS NULL) THEN 'FAIL' ELSE 'PASS' END dq_status
          FROM (SELECT * FROM raw_orders QUALIFY row_number() OVER(PARTITION BY order_id ORDER BY _loaded_at DESC)=1)""")
        # Merge increment: newest copy of an order wins in staging; raw remains append-only.
        if inc.exists():
            incdf=pd.read_csv(inc,dtype=str,keep_default_na=False); incdf["_loaded_at"]=datetime.now(timezone.utc).isoformat(); incdf["_source_file"]=INCREMENT; incdf["_run_id"]=run_id
            con.register("inc_input",incdf)
            con.execute("CREATE OR REPLACE TEMP TABLE inc_clean AS SELECT *,try_cast(order_ts AS TIMESTAMP) order_ts_cast,try_cast(quantity AS INTEGER) quantity_cast,try_cast(unit_price AS DOUBLE) unit_price_cast,try_cast(discount_pct AS DOUBLE) discount_pct_cast,try_cast(order_amount AS DOUBLE) order_amount_cast,try_cast(delivery_minutes AS DOUBLE) delivery_minutes_cast,upper(trim(order_status)) order_status_std,NULL::VARCHAR failure_reason,'PASS'::VARCHAR dq_status FROM inc_input")
            con.execute("CREATE OR REPLACE TEMP TABLE stg_orders_all AS SELECT * FROM stg_orders UNION ALL BY NAME SELECT * FROM inc_clean")
            con.execute("CREATE OR REPLACE TABLE stg_orders AS SELECT * FROM stg_orders_all QUALIFY row_number() OVER(PARTITION BY order_id ORDER BY _loaded_at DESC)=1")
            con.unregister("inc_input")
        con.execute("""CREATE OR REPLACE TABLE mart_daily_store_kpis AS
          SELECT CAST(o.order_ts_cast AS DATE) order_date,s.zone,s.store_id,s.store_name,
          SUM(CASE WHEN o.order_status_std='DELIVERED' THEN o.order_amount_cast ELSE 0 END) net_revenue,
          count(*) FILTER(WHERE o.order_status_std='DELIVERED') delivered_orders
          FROM stg_orders o LEFT JOIN stg_stores s USING(store_id) WHERE o.dq_status='PASS' GROUP BY ALL""")
        con.execute("""CREATE OR REPLACE TABLE mart_category_margin AS SELECT p.category,
          SUM(CASE WHEN o.order_status_std='DELIVERED' THEN o.order_amount_cast ELSE 0 END) revenue,
          SUM(CASE WHEN o.order_status_std='DELIVERED' THEN o.order_amount_cast ELSE 0 END)-SUM(CASE WHEN o.order_status_std='DELIVERED' THEN o.quantity_cast*p.cost_price ELSE 0 END) gross_margin
          FROM stg_orders o LEFT JOIN stg_products p USING(product_id) WHERE o.dq_status='PASS' GROUP BY p.category""")
        con.execute("""CREATE OR REPLACE TABLE mart_delivery_sla AS SELECT s.store_id,s.store_name,avg(o.delivery_minutes_cast) avg_delivery_minutes,
          avg(CASE WHEN o.delivery_minutes_cast<=15 THEN 1.0 ELSE 0.0 END)*100 pct_within_15_min
          FROM stg_orders o LEFT JOIN stg_stores s USING(store_id) WHERE o.dq_status='PASS' AND o.order_status_std='DELIVERED' GROUP BY ALL""")
        con.execute("""CREATE OR REPLACE TABLE mart_payment_reconciliation AS SELECT o.order_id,o.order_amount_cast order_amount,p.amount_paid,
          CASE WHEN p.order_id IS NULL THEN 'No payment found' WHEN abs(o.order_amount_cast-p.amount_paid)>0.02 THEN 'Amount mismatch' WHEN p.payment_status!='SUCCESS' THEN 'Payment not successful' END failure_reason
          FROM stg_orders o LEFT JOIN stg_payments p USING(order_id) WHERE o.dq_status='PASS' AND (p.order_id IS NULL OR abs(o.order_amount_cast-p.amount_paid)>0.02 OR p.payment_status!='SUCCESS')""")
        con.execute("""CREATE OR REPLACE TABLE mart_top_customers AS SELECT o.customer_id,c.loyalty_tier,sum(o.order_amount_cast) spend
          FROM stg_orders o LEFT JOIN stg_customers c USING(customer_id) WHERE o.dq_status='PASS' AND o.order_status_std='DELIVERED' GROUP BY ALL ORDER BY spend DESC LIMIT 10""")
        # DQ scorecard for the SQL staging rule set.
        con.execute("CREATE TABLE IF NOT EXISTS dq_results(run_id VARCHAR,dag_id VARCHAR,check_name VARCHAR,dimension VARCHAR,severity VARCHAR,rows_checked BIGINT,rows_failed BIGINT,status VARCHAR,checked_at TIMESTAMP)")
        bad=con.execute("SELECT count(*) FROM stg_orders WHERE dq_status='FAIL'").fetchone()[0]
        con.execute("INSERT INTO dq_results VALUES (?,?,?,?,?,?,?,?,?)",[run_id,"nammamart_elt_surbhi","staging_order_rules","Multiple","Critical",len(read_csv(FILES["orders"])),bad,"FAIL" if bad else "PASS",datetime.now(timezone.utc).replace(tzinfo=None)])
        loaded=con.execute("SELECT count(*) FROM stg_orders WHERE dq_status='PASS'").fetchone()[0]
        record_run(con,run_id,"nammamart_elt_surbhi",start,extracted,loaded,bad,"success")
    except Exception:
        record_run(con,run_id,"nammamart_elt_surbhi",start,extracted,loaded,0,"failed")
        raise
    finally: con.close()
