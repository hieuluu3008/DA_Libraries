from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import requests,pytz,json


#Information
dagid = 'control_dag_name'
PIC ="hieult"


def task_failure_callback(context):
    webhook_url = 'https://chat.googleapis.com/v1/spaces/XXX/messages?key=YYYY&token=ZZZZZ'
    dag_id = context['dag'].dag_id
    task_instance = context['task_instance']
    execution_date = context['execution_date']
    log_url=f"{task_instance.log_url}"
    log_url=log_url.replace("http://localhost:8080", "https://airflow-serving.company.com")
    message = {"text":f"""*DAG RUN FAILED:*\nDagId: {dag_id}\nTaskId: {task_instance.task_id}\nTime: {execution_date.astimezone(pytz.timezone('Asia/Ho_Chi_Minh'))}\nLog: {log_url}"""}
    response = requests.post(webhook_url, data=json.dumps(message), headers={"Content-Type": "application/json"})
    if response.status_code == 200:
        print('Message sent successfully!')
    else:
        print(f'Error: {response.status_code}, {response.text}')


default_args = {
'owner': 'BI_Team',
'depends_on_past': False,
'catchup':False,
'email_on_failure': False,
'email_on_retry': False,
'retries': 1,
'retry_delay': timedelta(minutes=10),
'start_date': datetime(2024, 10, 25),
'execution_timeout': timedelta(minutes=55),
"on_failure_callback": task_failure_callback
}


with DAG(
    dag_id= dagid,
    default_args= default_args,
    schedule='45 8,11,14,17,22 * * *',
    dagrun_timeout= timedelta(minutes= 120),
    description='Control DAG',
    catchup= False
) as dag:


    task1 = TriggerDagRunOperator(
        task_id='get_raw_data',
        trigger_dag_id='get_raw_data.py',  
        wait_for_completion=True,                  
    )


    task2 = TriggerDagRunOperator(
        task_id='etl_process',
        trigger_dag_id='etl_process.py',  
        wait_for_completion=True,                  
    )


    task1 >> task2