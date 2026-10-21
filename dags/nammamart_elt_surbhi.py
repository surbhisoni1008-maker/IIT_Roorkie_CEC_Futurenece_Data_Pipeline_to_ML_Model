"""NammaMart ELT pipeline for Surbhi. Raw source rows are append-only."""
import pendulum
from datetime import timedelta
from airflow.decorators import dag, task
from include.nammamart_pipeline import run_elt

@dag(
    dag_id="nammamart_elt_surbhi",
    description="Preserve raw NammaMart exports and transform with DuckDB SQL",
    schedule="0 9 * * 1-6",
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata"),
    catchup=False,
    default_args={"owner":"surbhi","retries":2,"retry_delay":timedelta(minutes=3),"execution_timeout":timedelta(minutes=30)},
    tags=["nammamart","elt","assignment"],
)
def nammamart_elt_surbhi():
    @task
    def extract_source_inventory():
        from include.nammamart_pipeline import data_dir, FILES, INCREMENT
        d=data_dir()
        missing=[fn for fn in [*FILES.values(), INCREMENT] if not (d/fn).exists()]
        if missing: raise FileNotFoundError(f"Missing source files in {d}: {missing}")
        return {"directory":str(d),"files":len(FILES)+1}
    @task
    def load_raw_transform_sql(inventory):
        from airflow.operators.python import get_current_context
        ctx=get_current_context()
        run_elt(str(ctx["run_id"]))
        return "ELT completed; raw/staging/mart tables are in DuckDB."
    inventory=extract_source_inventory()
    load_raw_transform_sql(inventory)
nammamart_elt_surbhi()
