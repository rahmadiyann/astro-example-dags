from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from pendulum import datetime
import requests

#Define the basic parameters of the DAG, like schedule and start_date
dag = DAG(
    "crons",
    start_date=datetime(2024, 4, 1, 0, 0),
    schedule="*/5 * * * *",
    catchup=True,
    default_args={"owner": "rian"},
    tags=["priority"],
)

#Define tasks

sites = {
    "rian.social": "https://www.rian.social/dontdie",
    "sw_auto": "https://www.rian.social/swauto/process",
    "track_resi": "https://www.rian.social/pakeeeet/track"
}

def call(url):
    r = requests.get(url)
    print(r.text)

start = DummyOperator(task_id="start", dag=dag)
end = DummyOperator(task_id="end", dag=dag)
for site, url in sites.items():
    call = PythonOperator(
        task_id=f"call_{site}",
        python_callable=call,
        op_kwargs={"url": url},
        dag=dag,
    )
    start >> call >> end
