from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from pendulum import datetime
import requests

#Define the basic parameters of the DAG, like schedule and start_date
dag = DAG(
    "dontdie",
    start_date=datetime(2024, 4, 1),
    schedule="*/5 * * * *",
    catchup=True,
    default_args={"owner": "rian"},
    tags=["example"],
)

#Define tasks

def call_dontdie():
    r = requests.get("http://www.rian.social/dontdie")
    print(r.text)

start = DummyOperator(task_id="start", dag=dag)
end = DummyOperator(task_id="end", dag=dag)
call = PythonOperator(
    task_id="call_dontdie",
    python_callable=call_dontdie,
    dag=dag,
)

start >> call >> end