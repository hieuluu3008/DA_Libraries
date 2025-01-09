from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.decorators import task_group
from datetime import datetime, timedelta

default_arguments = {
    'owner': 'user_name', #optional - hỗ trợ trong việc quản lý thông tin, hỗ trợ tìm kiếm trong DAG
    'retries': 1,  # Số lần thử lại khi task thất bại
    'retry_delay': timedelta(minutes=5),  # Thời gian chờ giữa các lần thử lại
    'start_date': datetime(2023, 1, 1)  # Ngày bắt đầu chạy DAG
}

dag = DAG(
	dag_id = 'dag_etl', # DAG name on UI
	default_args = default_arguments,
	description = 'trigger dag', # description on UI
	schedule_interval = '05 5 * * *' # thời gian chạy của DAG theo Cron
)

def sleep(time): # khai báo hàm để sử dụng trong PythonOperator
	time.sleep(time)

@task_group(group_id='raw_group')
def tg1():
    task1 = BashOperator(
        task_id='bash_raw_1',
        bash_command='echo “Example1!”', # command to run
        dag=dag,
        retries = 1,
        retry_delay = timedelta(minutes=5)
    )

    task2 = BashOperator(
        task_id = 'bash_raw_2',
        bash_command ='echo “Example2!”', # command to run
        dag=dag,
        retries = 1,
        retry_delay = timedelta(minutes=5)
    )
    task1 >> task2


@task_group(group_id='etl_group')
def tg2():  
    task3 = PythonOperator(
	task_id = 'sleep', # task name on UI
	python_callable = sleep, # hàm python muốn thực thi
	op_kwargs= {'time': 5}, # dictionary gồm các đối số {key:value} để truyền vào hàm
    dag=dag, 
    )

    task4 = PythonOperator(
	task_id = 'sleep',
	python_callable = sleep,
	op_kwargs= {'time': 10},
    dag=dag, 
    )
    task3 >> task4
    
tg1() >> tg2()
