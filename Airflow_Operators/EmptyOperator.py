from airflow.operators.empty import EmptyOperator
from airflow import DAG
from datetime import datetime

with DAG(
    dag_id="empty_operator_example",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    
    start >> end  # Simple DAG structure
