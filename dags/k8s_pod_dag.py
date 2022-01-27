import datetime as dt

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)

version = "0.1.28.0"
image = "docker.io/ivostoy/my-dbt:1.0.7"


def now():
    return dt.datetime.utcnow()


def seconds(sec):
    return dt.timedelta(seconds=sec)


default_args = {
    'owner': 'Ivo',
    'start_date': now(),
    'retries': 1,
    'retry_delay': seconds(300)
}

interval = dt.timedelta(seconds=600)
interval = None


def wait_result(ti):
    print("waiting result...")
    result = ti.xcom_pull(key='return_value', task_ids=['etl'])
    print(f"result {result}")
    return result


print(f'k8s_pod etl v.{version} start,  {now()}')

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

    get_result = PythonOperator(
        task_id='wait_result',
        python_callable=wait_result
    )

    # execute etl task in K8S POD
    etl >> get_result

    print(f'k8s_pod end,  {now()}')
