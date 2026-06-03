from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from src.load_from_s3 import load_from_s3
from src.upload_to_clickhouse import upload_to_clickhouse


DEFAULT_ARGS = {
    "owner": "not_so_bad",
    "retries": 2,
    "retry_delay": 600,
    "start_date": datetime(2026, 5, 14),
}

with DAG(
    dag_id="not_so_bad_weekly_visits_to_clickhouse",
    tags=["not_so_bad", "metrika", "clickhouse"],
    schedule="0 6 * * 1",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    max_active_tasks=1,
) as dag:

    dag_start = EmptyOperator(task_id="dag_start")
    dag_end = EmptyOperator(task_id="dag_end")

    load_from_s3_task = PythonOperator(
        task_id="load_from_s3",
        python_callable=load_from_s3,
    )

    upload_to_clickhouse_task = PythonOperator(
        task_id="upload_to_clickhouse",
        python_callable=upload_to_clickhouse,
    )

    dag_start >> load_from_s3_task >> upload_to_clickhouse_task >> dag_end
