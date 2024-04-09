
import pendulum
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
import requests

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
dagname = "track_resi"
dag = DAG(
    dagname,
    default_args=default_args,
    description=dagname,
    schedule_interval='@hourly',
    catchup=False,
    tags=['priority']
)
start = DummyOperator(
    task_id='start',
    dag=dag,
)

def call():
    response = requests.get("https://rian.social/api/cekresi/track")
    print(response.text)

execute = PythonOperator(
    task_id='execute',
    python_callable=call,
    dag=dag,
)

end = DummyOperator(
    task_id='end',
    dag=dag,
)

start >> execute >> end
