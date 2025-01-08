from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import timedelta
from airflow.operators.empty import EmptyOperator

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

start_task = EmptyOperator(
	task_id = 'start_task',
	dag = dag,
)

def branch_test(**kwargs):
	if int(kwargs['ds_nodash']) % 2 == 0:
		return 'even_day_task' # task_id 1
	else:
		return 'odd_day_task' # task_id 2
branch_task = BranchPythonOperator(
task_id = 'branch_task',
				provide_context = True, #optional
				python_callable = branch_test, # hàm python muốn thực thi
				dag = dag,
				)

even_day_task = EmptyOperator(
	task_id = 'even_day_task',
	dag = dag,
)

odd_day_task = EmptyOperator(
	task_id = 'odd_day_task',
	dag = dag,
)

follow_up_task = EmptyOperator(
	task_id = 'follow_up_task',
	dag = dag,
)

start_task >> branch_task >> [ even_day_task, odd_day_task ] >> follow_up_task
