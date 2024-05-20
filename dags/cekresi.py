from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from pendulum import datetime
import requests

# Define the basic parameters of the DAG, like schedule and start_date
dag = DAG(
    "hourly_cron",
    start_date=datetime(2024, 4, 1, 0, 0),
    # set schedule to every 55 minute
    schedule="55 * * * *",
    catchup=False,
    default_args={"owner": "rian"},
    tags=["priority"],
)

# Define sites
sites = {
    "track_resi": "https://www.rian.social/pakeeeet/track",
    # "spotify_history": "https://www.rian.social/discover-weekly/"
}

# Create a function to make a request to a URL
def make_request(url):
    r = requests.get(url)
    print(f"Response: {r.text}")

# Define tasks
start = DummyOperator(task_id="start", dag=dag)
end = DummyOperator(task_id="end", dag=dag)
for site, url in sites.items():
    call_site = PythonOperator(
        task_id=f"call_{site}",
        python_callable=make_request,
        op_kwargs={"url": url},
        dag=dag,
    )
    start >> call_site >> end
