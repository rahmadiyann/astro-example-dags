
import pendulum
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
import random
from scripts.dbops import update_end_time, update_status
import time

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
dagname = "cbs_treasury_to_ods"
dag = DAG(
    dagname,
    default_args=default_args,
    description=dagname,
    schedule_interval='30 3 * * *',
    catchup=False,
    tags=['priority']
)
start = PythonOperator(
    task_id='start',
    python_callable=update_status,
    op_kwargs={"dagname": dagname},
    dag=dag,
)
done = PythonOperator(
    task_id='done',
    python_callable=update_end_time,
    op_kwargs={"dagname": dagname},  # Pass your parameter here as a string
    dag=dag,
)
def run(a,b):
    #random sleep in minute
    start = a*60
    end = b*60
    sleep = random.randint(start, end)
    print("I'm going to sleep for " + str(sleep) + " second")
    time.sleep(sleep)
    print("I woke up after " + str(sleep) + " second")

execute = PythonOperator(
    task_id='execute',
    python_callable=run,
    op_kwargs={"a": 1, "b": 15},
)

start >> execute >> done
