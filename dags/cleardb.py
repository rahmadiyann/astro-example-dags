import pendulum
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from scripts.dbops import clear_end_time

local_tz = pendulum.timezone('Asia/Jakarta')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 3, tzinfo=local_tz),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
dagname = "clear_db"
dag = DAG(
    dagname,
    default_args=default_args,
    description=dagname,
    schedule_interval='0 11 * * *',
    catchup=False,
    tags=['priority']
)
start = DummyOperator(
    task_id='start',
    dag=dag,
)
clear = PythonOperator(
    task_id='clear',
    python_callable=clear_end_time,
    dag=dag,
)
end = DummyOperator(
    task_id='end',
    dag=dag,
)

start >> clear >> end