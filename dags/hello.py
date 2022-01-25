from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def print_hello():
    msg = f'Hello world, time now: {datetime.now()}'
    print(msg)
    return msg


now = datetime.now()
cron = '0 * * * *'  # execute every hour at :00
cron = '* /5 * * *'  # execute every 5th minute
start = now
dag = DAG('hello_world', description='Hello World DAG',
          schedule_interval=cron,
          start_date=start, catchup=False)

hello_operator = PythonOperator(task_id='hello_task', python_callable=print_hello, dag=dag)

print(f'starting: {now}, schedule: {cron}, start: {now}')

hello_operator
