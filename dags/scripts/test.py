from send_message import *
from dbops import fetch_status

def send_msg():
    statuses = fetch_status()
    # print(statuses)
    status_message = job_status_message(statuses)
    # print(status_message)
    send_discord_message(status_message)

send_msg()