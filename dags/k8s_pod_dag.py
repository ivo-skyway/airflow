import datetime as dt
import time

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)

version = "0.1.26.0"

default_args = {
    'owner': 'Ivo',
    'start_date': dt.datetime(2022, 1, 1),
    'retries': 1,
    'retry_delay': dt.timedelta(seconds=300)
}

with DAG('etl_dag',
         default_args=default_args,
         catchup=False,
         schedule_interval=dt.timedelta(seconds=600)) as dag:
    etl = KubernetesPodOperator(
        namespace='default',
        image="docker.io/ivostoy/my-dbt:1.0.0",
        cmds=[],
        arguments=[],
        labels={"foo": "bar"},
        name="etl",
        task_id="etl",
        get_logs=True,
        log_events_on_failure=True,
        in_cluster=True,
    )

    print(etl.dry_run())
    time.sleep(10)

    print(f'k8s_pod etl v.{version} start,  {dt.datetime.now()}')

    etl

    time.sleep(10)

    print(f'k8s_pod end  {dt.datetime.now()}')
