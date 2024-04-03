
import pendulum
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from scripts.send_message import send_discord_message
import random
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
dagname = "cbs_mspayment_intraday"
dag = DAG(
    dagname,
    default_args=default_args,
    description=dagname,
    schedule_interval='30 8-17 * * *',
    catchup=False,
    tags=['priority']
)

def send_message():
    # time now in Asia/Jakarta
    now = datetime.now(local_tz)
    # reformat to HH:MM:SS
    now_reformat = now.strftime("%H:%M:%S")
    send_discord_message(f"{dagname} is finished at {now_reformat}")

start = DummyOperator(
    task_id='start',
    dag=dag,
)

done = PythonOperator(
    task_id='done',
    python_callable=send_message,
    op_kwargs={"dagname": dagname},  # Pass your parameter here as a string
    dag=dag,
)

def run(a,b):
    #random sleep in minute
    start = a*60
    end = b*60
    sleep = random.randint(start, end)
    print("I'm going to sleep for " + str(round(sleep/60)) + " minute")
    time.sleep(sleep)
    print("I woke up after " + str(round(sleep/60)) + " minute")

execute = PythonOperator(
    task_id='execute',
    python_callable=run,
    op_kwargs={"a": 20, "b": 30},
)

start >> execute >> done
