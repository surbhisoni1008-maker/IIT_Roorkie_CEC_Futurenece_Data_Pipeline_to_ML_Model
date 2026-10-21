"""NammaMart ETL pipeline for Surbhi. Source data is never modified."""
import pendulum
from datetime import timedelta
from airflow.decorators import dag, task
from include.nammamart_pipeline import run_etl

@dag(
    dag_id="nammamart_etl_surbhi",
    description="Validate and transform NammaMart exports before loading finance tables",
    schedule="0 9 * * 1-6",
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata"),
    catchup=False,
    default_args={"owner":"surbhi","retries":2,"retry_delay":timedelta(minutes=3),"execution_timeout":timedelta(minutes=30)},
    tags=["nammamart","etl","assignment"],
)
def nammamart_etl_surbhi():
    @task
    def extract_source_inventory():
        # Fail early with a clear message if the expected dataset is absent.
        from include.nammamart_pipeline import data_dir, FILES, INCREMENT
        d=data_dir()
        missing=[fn for fn in [*FILES.values(), INCREMENT] if not (d/fn).exists()]
        if missing: raise FileNotFoundError(f"Missing source files in {d}: {missing}")
        return {"directory":str(d),"files":len(FILES)+1}
    @task
    def validate_transform_load(inventory):
        from airflow.operators.python import get_current_context
        ctx=get_current_context()
        run_etl(str(ctx["run_id"]))
        return "ETL completed; inspect DuckDB tables, quarantine, and reports."
    inventory=extract_source_inventory()
    validate_transform_load(inventory)
nammamart_etl_surbhi()
