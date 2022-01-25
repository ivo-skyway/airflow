from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

now = datetime.now()
cron = '0 * * * *'  # execute every hour at :00
cron = '*/15 * * * *'  # execute every 5th minute
start = now
version = "0.1.25.1"


def print_hello():
    msg = f'Hello world v.{version} time now: {datetime.now()}'
    print(msg)
    return msg


dag = DAG('hello_world', description=f'Hello World DAG v.{version}',
          schedule_interval=cron,
          start_date=start, catchup=False)

hello_operator = PythonOperator(task_id='hello_task', python_callable=print_hello, dag=dag)

print(f'Hello v.{version} starting: {now}, schedule: {cron}, start: {now}')

hello_operator
