

from datetime import datetime
from airflow import DAG 
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator 

from api_to_postgre_lanit_test.src.load_to_postgre import load_to_postgre

DEFAULT_ARGS = {
    "owner": "tkachev_roman",
    "retries": 2, 
    "retry_delay": 600,
    "start_date": datetime(2026, 6, 18)
}

with DAG( 
    dag_id="tkachev_load_update_leads",
    tags=["tkachev", "leads", "api", "postgre"],
    schedule="@daily",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    max_active_tasks=1,
    catchup=False
) as dag: 
    
    dag_start = EmptyOperator(task_id="dag_start")
    dag_end = EmptyOperator(task_id="dag_end")

    load_to_postgre = PythonOperator(
        task_id="load_to_postgre",
        python_callable=load_to_postgre
    )

    dag_start >> load_to_postgre >> dag_end
