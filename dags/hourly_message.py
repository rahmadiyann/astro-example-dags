import pendulum
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from scripts.send_message import *
from scripts.dbops import fetch_status

local_tz = pendulum.timezone('Asia/Jakarta')

default_args = {
    'owner': 'rian',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 1, tzinfo=local_tz),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
dagname = "hourly_message"
dag = DAG(
    dagname,
    default_args=default_args,
    description=dagname,
    schedule_interval='0 23-23,0-9 * * *',
    catchup=False,
    tags=['priority']
)
start = DummyOperator(
    task_id='start',
    dag=dag,
)
end = DummyOperator(
    task_id='end',
    dag=dag,
)

def send_msg():
    statuses = fetch_status()
    status_message = job_status_message(statuses)
    send_discord_message(status_message)

send_message = PythonOperator(
    task_id='send_message',
    python_callable=send_msg,
    dag=dag,
)

start >> send_message >> end