from datetime import datetime
from airflow import DAG
from airflow.operators.email import EmailOperator
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

# EmailOperator
email_task = EmailOperator(
	task_id = 'email_example', # name in UI
	to = 'luutrunghieu298@gmail.com', # send email to
	subject = 'Email DAG Airflow', # subject of email
	html_content = 'Something went wrong with the dag', # content of email
	dag=dag,
	trigger_rule = 'all_done'
)
