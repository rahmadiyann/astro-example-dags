
import pendulum
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
import random
from scripts.dbops import update_end_time
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
dagname = "cbs_mspayment"
dag = DAG(
    dagname,
    default_args=default_args,
    description=dagname,
    schedule_interval='0 3 * * *',
    catchup=False,
    tags=['priority']
)
start = DummyOperator(
    task_id='start',
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
    print("I'm going to sleep for " + str(sleep) + " minute")
    time.sleep(sleep)
    print("I woke up after " + str(sleep) + " minute")

execute = PythonOperator(
    task_id='execute',
    python_callable=run,
    op_kwargs={"a": 1, "b": 5},
)

start >> execute >> done
