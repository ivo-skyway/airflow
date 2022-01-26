import datetime as dt

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)

version = "0.1.26.0"

default_args = {
    'owner': 'Ivo',
    'start_date': dt.datetime(2022, 1, 1),
    'retries': 3,
    'retry_delay': dt.timedelta(seconds=30)
}

with DAG('etl_dag',
         default_args=default_args,
         catchup=False,
         schedule_interval=dt.timedelta(seconds=60)) as dag:
    etl = KubernetesPodOperator(
        namespace='airflow',
        image="docker.io/ivostoy/my-dbt:1.0.0",
        cmds=["--version"],
        arguments=[],
        labels={"foo": "bar"},
        name="etl",
        task_id="etl",
        get_logs=True,
        log_events_on_failure=True,
        in_cluster=True,
    )

    print(etl.dry_run())

    print(f'k8s_pod etl v.{version} start,  {dt.datetime.now()}')

    etl

    dt.time.sleep(10)

    print(f'k8s_pod end  {dt.datetime.now()}')
