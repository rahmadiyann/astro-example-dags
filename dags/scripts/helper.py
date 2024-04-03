import random
import time

def seconds_to_hms(seconds):
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60
    
    time_string = ""
    if hours > 0:
        time_string += f"{hours} Hour{'s' if hours > 1 else ''} "
    if minutes > 0:
        time_string += f"{minutes} Minute{'s' if minutes > 1 else ''} "
    if seconds > 0:
        time_string += f"{seconds} Second{'s' if seconds > 1 else ''}"
    
    return time_string.strip()

def run(a,b):
    #random sleep in minute
    start = a*60
    end = b*60
    sleep = random.randint(start, end)
    timestring = seconds_to_hms(sleep)
    print(f"Sleeping for {timestring}")
    time.sleep(sleep)
    print(f"I woke up after {timestring}")

def pyops(a, b):
    return PythonOperator(
        task_id='execute',
        python_callable=run,
        op_kwargs={"a": a, "b": b},
    )