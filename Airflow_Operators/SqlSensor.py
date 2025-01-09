from datetime import datetime
from airflow import DAG
from airflow.sensors.sql import SqlSensor
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

wait_for_dynamic_condition = SqlSensor(
    task_id="wait_for_sql_condition",
    conn_id="my_mysql_connection",
    sql="SELECT COUNT(*) FROM my_table WHERE date = %(target_date)s;",
    parameters={"target_date": "2024-08-30"},  # Pass parameters to the query
    poke_interval=15,  # Check every 15 seconds
    timeout=300,  # Timeout after 5 minutes
    mode="reschedule",  # Reschedule mode for long waits
)


