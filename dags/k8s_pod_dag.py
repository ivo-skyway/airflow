import datetime as dt

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)

####################
version = "0.1.27.7"
image = "docker.io/ivostoy/my-dbt:1.0.6"
####################

default_args = {
    'owner': 'Ivo',
    'start_date': dt.datetime(2022, 1, 1),
    'retries': 1,
    'retry_delay': dt.timedelta(seconds=300)
}

interval = dt.timedelta(seconds=600)
interval = None


def wait_result(ti):
    print("waiting result...")
    result = ti.xcom_pull(key='return_value', task_ids=['etl'])
    print(f"result {result}")
    return result


print(f'k8s_pod etl v.{version} start,  {dt.datetime.now()}')

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

    # print(f"before run")
    # etl
    # print(f"after run")

    # result = etl.xcom_pull(key='return_value', task_ids=['etl'])
    # print(f"result {result}")

    get_result = PythonOperator(
        task_id='wait_result',
        python_callable=wait_result
    )
    # EXECUTE AS K8S POD
    etl >> get_result

    # sleep(30)
    #
    print(f'k8s_pod end,  {dt.datetime.now()}')
