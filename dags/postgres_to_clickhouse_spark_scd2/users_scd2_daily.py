from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from src.apply_scd2 import apply_scd2
from src.load_from_postgres import load_from_postgres
from src.upload_to_clickhouse import upload_to_clickhouse


DEFAULT_ARGS = {
    "owner": "not_so_bad",
    "retries": 2,
    "retry_delay": 600,
    "start_date": datetime(2026, 5, 14),
}

with DAG(
    dag_id="not_so_bad_users_scd2_to_clickhouse",
    tags=["not_so_bad", "postgres", "spark", "clickhouse", "scd2"],
    schedule="0 6 * * *",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    max_active_tasks=1,
) as dag:

    dag_start = EmptyOperator(task_id="dag_start")
    dag_end = EmptyOperator(task_id="dag_end")

    load_from_postgres = PythonOperator(
        task_id="load_from_postgres",
        python_callable=load_from_postgres,
    )

    apply_scd2 = PythonOperator(
        task_id="apply_scd2",
        python_callable=apply_scd2,
    )

    upload_to_clickhouse = PythonOperator(
        task_id="upload_to_clickhouse",
        python_callable=upload_to_clickhouse,
    )

    dag_start >> load_from_postgres >> apply_scd2 >> upload_to_clickhouse >> dag_end
    
