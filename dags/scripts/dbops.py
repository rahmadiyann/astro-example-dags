import psycopg2
import os
from datetime import datetime

conn = psycopg2.connect(
    dbname=os.environ.get('DATABASE_NAME'),
    user=os.environ.get('DATABASE_USER'),
    password=os.environ.get('DATABASE_PASSWORD'),
    host=os.environ.get('DATABASE_HOST'),
)

#get the name from table priority_job
def update_end_time(context, dagname):
    end_time = datetime.now().strftime("%H:%M")
    cur = conn.cursor()
    cur.execute("UPDATE priority_job SET end_time = %s, status = 'DONE' WHERE job_name = %s", (str(end_time), dagname))
    conn.commit()
    cur.close()

def clear_end_time():
    cur = conn.cursor()
    cur.execute("UPDATE priority_job SET end_time = NULL")
    conn.commit()
    cur.close()

def fetch_status():
    cur = conn.cursor()
    cur.execute("SELECT id, name, start_time, end_time, status FROM priority_job ORDER BY id ASC")
    status_tuples = cur.fetchall()
    cur.close()
    jobs = []
    for status_tupe in status_tuples:
        id, name, start_time, end_time, status = status_tupe
        job_dict = {
            "id": id,
            "name": name,
            "start_time": start_time,
            "end_time": end_time,
            "status": status
        }
        jobs.append(job_dict)
    # print(jobs)
    return jobs


#get the name from table priority_job

# def get_priority_job():
#     cur = conn.cursor()
#     cur.execute("SELECT name, start_time FROM priority_job ORDER BY id")
#     names_tuples = cur.fetchall()
#     cur.close()
#     jobs = []
#     for name_tuple in names_tuples:
#         name, start_time = name_tuple
#         hour, minute = map(int, start_time.split(':'))
#         job_dict = {
#             "name": name,
#             "hour": hour,
#             "minute": minute
#         }
#         jobs.append(job_dict)
#     return jobs

# names = get_priority_job()
# for name in names:
#     #create a new .py file for each name
#     with open(f'/Users/rian/Documents/GitHub/astro-example-dags/dags/{name["name"]}.py', 'w') as f:
#         file_content = f"""
# import pendulum
# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# from airflow.operators.dummy_operator import DummyOperator
# from datetime import datetime, timedelta
# import random
# from scripts.dbops import update_end_time
# import time

# local_tz = pendulum.timezone('Asia/Jakarta')

# default_args = {{
#     'owner': 'rian',
#     'depends_on_past': False,
#     'start_date': datetime(2024, 4, 1, tzinfo=local_tz),
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }}
# dagname = "{name['name']}"
# dag = DAG(
#     dagname,
#     default_args=default_args,
#     description=dagname,
#     schedule_interval='{name['minute']} {name['hour']} * * *',
#     catchup=False,
#     tags=['priority']
# )
# start = DummyOperator(
#     task_id='start',
#     dag=dag,
# )
# done = PythonOperator(
#     task_id='done',
#     python_callable=update_end_time,
#     op_kwargs={{"dagname": dagname}},  # Pass your parameter here as a string
#     dag=dag,
# )
# def run(a,b):
#     #random sleep in minute
#     start = a*60
#     end = b*60
#     sleep = random.randint(start, end)
#     print("I'm going to sleep for " + str(sleep) + " minute")
#     time.sleep(sleep)
#     print("I woke up after " + str(sleep) + " minute")

# execute = PythonOperator(
#     task_id='execute',
#     python_callable=run,
#     op_kwargs={{"a": 1, "b": 5}},
# )

# start >> execute >> done
# """
#         f.write(file_content)
