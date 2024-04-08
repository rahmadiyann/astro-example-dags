from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from pendulum import datetime
import requests

#Define the basic parameters of the DAG, like schedule and start_date
dag = DAG(
    "dontdie",
    start_date=datetime(2024, 4, 1, 0, 0),
    schedule="*/10 * * * *",
    catchup=True,
    default_args={"owner": "rian"},
    tags=["example"],
)

#Define tasks

sites = {
    "rian.social": "http://www.rian.social/dontdie",
    # "rahmadiyan_backend": "https://riansbackend.onrender.com/dontdie"
}

def call_dontdie(url):
    r = requests.get("http://www.rian.social/dontdie")
    print(r.text)

start = DummyOperator(task_id="start", dag=dag)
end = DummyOperator(task_id="end", dag=dag)
for site, url in sites.items():
    call = PythonOperator(
        task_id=f"call_{site}",
        python_callable=call_dontdie,
        op_kwargs={"url": url},
        dag=dag,
    )
    start >> call >> end
