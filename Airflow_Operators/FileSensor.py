from datetime import datetime
from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from datetime import timedelta

default_arguments = {
    'owner': 'user_name', #optional - hỗ trợ trong việc quản lý thông tin, hỗ trợ tìm kiếm trong DAG
    'retries': 1,  # Số lần thử lại khi task thất bại
    'retry_delay': timedelta(minutes=5),  # Thời gian chờ giữa các lần thử lại
    'start_date': datetime(2023, 1, 1)  # Ngày bắt đầu chạy DAG
}

dag = DAG(
	dag_id = 'file_sensor_example', # DAG name on UI
	default_args = default_arguments,
	description = 'dag airflow', # description on UI
	schedule_interval = '@daily' # thời gian chạy của DAG theo Cron
)

# Waits for file(s) to be available in local File System
file_sensor_task = FileSensor(
	task_id = 'wait_for_file',
	filepath = 'C:/hieult/data/sales/salesdata.csv', # đường dẫn đến tệp cần kiểm tra
	poke_interval = 60, # (mặc định là 60 giây), chu kỳ kiểm tra sự tồn tại của tệp
	timeout = 300, # thời gian chờ tối đa cho đến khi tệp xuất hiện
	mode = 'poke', # Chế độ mặc định, giữ task đang thực thi và kiểm tra liên tục cho đến khi tệp xuất hiện
dag = dag,
)
