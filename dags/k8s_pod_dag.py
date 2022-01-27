import datetime as dt
import time

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)

####################
version = "0.1.27.A"
image = "docker.io/ivostoy/my-dbt:1.0.4"
####################

default_args = {
    'owner': 'Ivo',
    'start_date': dt.datetime(2022, 1, 1),
    'retries': 1,
    'retry_delay': dt.timedelta(seconds=300)
}

interval = dt.timedelta(seconds=600)
interval = None

with DAG('etl_dag',
         default_args=default_args,
         catchup=False,
         schedule_interval=interval) as dag:
    etl = KubernetesPodOperator(
        namespace='airflow',
        image=image,
        cmds=[],
        arguments=[],
        labels={"foo": "bar"},
        name="etl",
        task_id="etl",
        get_logs=True,
        log_events_on_failure=True,
        in_cluster=True,
        do_xcom_push=True
    )

    print(etl.dry_run())
    time.sleep(2)

    print(f'k8s_pod etl v.{version} start,  {dt.datetime.now()}')

    print("waiting result...")

    pod_task_xcom_result = BashOperator(
        bash_command="echo \"{{ task_instance.xcom_pull('etl')[0] }}\"",
        task_id="pod_task_xcom_result"
    )
    # EXECUTE AS K8S POD
    etl >> pod_task_xcom_result
    #
    print(f'k8s_pod end  {dt.datetime.now()}')
