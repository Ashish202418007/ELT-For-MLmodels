from airflow import DAG
from airflow.utils.dates import days_ago
from astronomer.providers.dbt.cloud.operators.dbt import DbtRunOperator
from datetime import timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

PROFILE_DIR = "/usr/local/airflow/dags/dbt" 
DBT_PROJECT_DIR = "/usr/local/airflow/dags/dbt/reviews"


with DAG(
    dag_id="beer_reviews_backfill",
    default_args=default_args,
    description="DBT DAG to backfill all historical beer reviews",
    schedule_interval=None,  
    start_date=days_ago(1),
    catchup=False,
    tags=["dbt", "backfill"],
) as dag_backfill:

    dbt_backfill = DbtRunOperator(
        task_id="dbt_run_backfill",
        dir=DBT_PROJECT_DIR,
        profiles_dir=PROFILE_DIR,
        vars={"mode": "backfill"},
    )


with DAG(
    dag_id="beer_reviews_incremental",
    default_args=default_args,
    description="DBT DAG to incrementally load beer reviews daily",
    schedule_interval="@daily",  
    start_date=days_ago(1),
    catchup=False,
    tags=["dbt", "incremental"],
) as dag_incremental:

    dbt_incremental = DbtRunOperator(
        task_id="dbt_run_incremental",
        dir=DBT_PROJECT_DIR,
        profiles_dir=PROFILE_DIR,
        vars={"mode": "incremental"},
    )
