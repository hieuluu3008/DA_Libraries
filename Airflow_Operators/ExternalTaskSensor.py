from datetime import datetime
from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import timedelta

default_arguments = {
    'owner': 'user_name', #optional - hỗ trợ trong việc quản lý thông tin, hỗ trợ tìm kiếm trong DAG
    'retries': 1,  # Số lần thử lại khi task thất bại
    'retry_delay': timedelta(minutes=5),  # Thời gian chờ giữa các lần thử lại
    'start_date': datetime(2023, 1, 1)  # Ngày bắt đầu chạy DAG
}

dag = DAG(
	dag_id = 'email_example', # DAG name on UI
	default_args = default_arguments,
	description = 'email example', # description on UI
	schedule_interval = '05 5 * * *' # thời gian chạy của DAG theo Cron
)

# create dependencies between tasks in different DAGs
wait_for_task = ExternalTaskSensor(
    task_id="wait_for_external_task",
    external_dag_id="external_dag",  # The DAG to monitor
    external_task_id="external_task",  # The task to monitor
    poke_interval=30,  # Check every 30 seconds
    timeout=600,  # Timeout after 10 minutes
    mode="poke",  # Use poke mode
)

