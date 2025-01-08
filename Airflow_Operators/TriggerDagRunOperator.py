from datetime import datetime
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import timedelta

default_arguments = {
    'owner': 'user_name', #optional - hỗ trợ trong việc quản lý thông tin, hỗ trợ tìm kiếm trong DAG
    'retries': 1,  # Số lần thử lại khi task thất bại
    'retry_delay': timedelta(minutes=5),  # Thời gian chờ giữa các lần thử lại
    'start_date': datetime(2023, 1, 1)  # Ngày bắt đầu chạy DAG
}

dag = DAG(
	dag_id = 'dag_etl', # DAG name on UI
	default_args = default_arguments,
	description = 'dag airflow', # description on UI
	schedule_interval = '05 5 * * *' # thời gian chạy của DAG theo Cron
)

task_1 = TriggerDagRunOperator(
    task_id='trigger_dag', # task name on UI
    trigger_dag_id='dag_raw_sales',  # dag_id want to trigger
    wait_for_completion=True # wait for the triggered dag to complete        
)

task_2 = TriggerDagRunOperator(
    task_id='trigger_dag', # task name on UI
    trigger_dag_id='dag_etl_sales',  # dag_id want to trigger
    wait_for_completion=True # wait for the triggered dag to complete        
)