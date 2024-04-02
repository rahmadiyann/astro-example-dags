from datetime import datetime
import requests

def job_status_message(statuses):
    # todays date in format 03/Apr/2024
    today = datetime.now().strftime("%d/%b/%Y")
    # time now in format 01:00
    time_now = datetime.now().strftime("%H:%M")
    header_message = f"""Dear all,
Berikut update status job priority airflow
*{today} - {time_now}*
"""
    for job in statuses:
        message = f"""
{job['id']}. {job['name']}
{job['start_time']} - {job['end_time'] if job['end_time'] else ''}
*{job['status']}*
"""
        header_message += message
    return header_message

def send_discord_message(message):
    webhook_url = 'https://discord.com/api/webhooks/1205259372738121748/Cjs6O5o6Z7VTLyhzJZQuGjWJKgKAvuFzlKtapi223ZYXLM_iOApNF3MeGI12OaehX_Sn'
    headers = {
        'Content-Type': 'application/json',
    }
    data = {
        'content': message,
    }
    response = requests.post(webhook_url, headers=headers, json=data)
    response.raise_for_status()