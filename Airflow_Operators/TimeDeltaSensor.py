from datetime import datetime
from airflow import DAG
from airflow.sensors.time_delta import TimeDeltaSensor
from datetime import timedelta

default_arguments = {
    'owner': 'user_name', #optional - hỗ trợ trong việc quản lý thông tin, hỗ trợ tìm kiếm trong DAG
    'retries': 1,  # Số lần thử lại khi task thất bại
    'retry_delay': timedelta(minutes=5),  # Thời gian chờ giữa các lần thử lại
    'start_date': datetime(2023, 1, 1)  # Ngày bắt đầu chạy DAG
}

# used to delay the execution of a task
wait_for_1_minute = TimeDeltaSensor(
    task_id="wait_for_1_minute",
    delta=timedelta(minutes=1)  # Wait for 5 minutes
    )
