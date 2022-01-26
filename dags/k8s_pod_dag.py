from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)

# k = KubernetesPodOperator(
#     name="hello-k8s",
#     image="python:3.8-slim-buster",
#     cmds=["bash", "-cx"],
#     arguments=["echo", "10"],
#     labels={"foo": "bar"},
#     task_id="kube_demo",
#     do_xcom_push=True,
# )
# k.dry_run()

default_args = {
    'owner': 'Ivo',
    'start_date': datetime(2022, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG('etl_dag',
         default_args=default_args,
         catchup=False,
         schedule_interval=None) as dag:
    extract_transform = KubernetesPodOperator(
        namespace='default',
        image="python:3.8-slim-buster",
        cmds=["echo"],
        arguments=["Hello"],
        labels={"foo": "bar"},
        name="extract-transform",
        task_id="extract-transform",
        get_logs=True,
        log_events_on_failure=True,
        in_cluster=True,
    )

    extract_transform
