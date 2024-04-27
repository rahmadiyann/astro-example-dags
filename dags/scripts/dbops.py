import psycopg2
import os
from datetime import datetime
import pendulum

local_tz = pendulum.timezone('Asia/Jakarta')
timenowinjakarta = datetime.now().astimezone(local_tz)
#time in HH:MM format
timenow = timenowinjakarta.strftime("%H:%M")


conn = psycopg2.connect(
    dbname=os.environ.get('DATABASE_NAME'),
    user=os.environ.get('DATABASE_USER'),
    password=os.environ.get('DATABASE_PASSWORD'),
    host=os.environ.get('DATABASE_HOST'),
)

#get the name from table priority_job
def update_end_time(dagname):
    cur = conn.cursor()
    try:
        cur.execute("UPDATE priority_job SET end_time = %s, status = 'DONE' WHERE name = %s", (str(timenow), dagname))
        conn.commit()
        print(f"Update end time for {dagname}")
    except Exception as e:
        print(f"Error updating end time for {dagname}: {e}")
    cur.close()

def update_status(dagname):
    cur = conn.cursor()
    try:
        cur.execute("UPDATE priority_job SET status = 'RUNNING' WHERE name = %s", (dagname,))
        conn.commit()
    except Exception as e:
        print(f"Error updating status for {dagname}: {e}")
    cur.close()

def clear_end_time():
    cur = conn.cursor()
    try:
        cur.execute("UPDATE priority_job SET end_time = NULL, status = 'WAITING'")
        conn.commit()
    except Exception as e:
        print(f"Error clearing end time: {e}")
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
# from scripts.dbops import update_end_time, update_status
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
# start = PythonOperator(
#     task_id='start',
#     python_callable=update_status,
#     op_kwargs={{"dagname": dagname}},
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
#     print("I'm going to sleep for " + str(sleep) + " second")
#     time.sleep(sleep)
#     print("I woke up after " + str(sleep) + " second")

# execute = PythonOperator(
#     task_id='execute',
#     python_callable=run,
#     op_kwargs={{"a": 1, "b": 5}},
# )

# start >> execute >> done
# """
#         f.write(file_content)
