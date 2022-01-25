import json
from datetime import datetime

from airflow.decorators import dag, task


@dag(schedule_interval=None, start_date=datetime(2021, 1, 1), catchup=False, tags=['example'])
def tutorial_taskflow_api_etl():
    """
    ### TaskFlow API Tutorial Documentation
    This is a simple ETL data pipeline example which demonstrates the use of
    the TaskFlow API using three simple tasks for Extract, Transform, and Load.
    Documentation that goes along with the Airflow TaskFlow API tutorial is
    located
    [here](https://airflow.apache.org/docs/apache-airflow/stable/tutorial_taskflow_api.html)
    """

    @task()
    def extract():
        """
        #### Extract task
        A simple Extract task to get data ready for the rest of the data
        pipeline. In this case, getting data is simulated by reading from a
        hardcoded JSON string.

        If you have tasks that require complex or conflicting requirements then you will have the ability to use the
        TaskFlow API with either a Docker container (since version 2.2.0) or Python virtual environment (since 2.0.2).
        This added functionality will allow a much more comprehensive range of use-cases for the TaskFlow API,
        as you will not be limited to the packages and system libraries of the Airflow worker.

        """
        data_string = '{"1001": 10, "1002": 20, "1003": 30}'

        order_data_dict = json.loads(data_string)
        return order_data_dict

    @task.docker(image='python:3.9-slim-buster', multiple_outputs=True)
    def transform(order_data_dict: dict):
        """
        #### Transform task
        A simple Transform task which takes in the collection of order data and
        computes the total order value.
        """
        total_order_value = 0

        for value in order_data_dict.values():
            total_order_value += value

        return {"total_order_value": total_order_value}

    def load(total_order_value: float):
        """
        #### Load task
        A simple Load task which takes in the result of the Transform task and
        instead of saving it to end user review, just prints it out.
        """

        print(f"Total order value is: {total_order_value:.2f}")

    order_data = extract()
    order_summary = transform(order_data)
    load(order_summary["total_order_value"])


tutorial_etl_dag = tutorial_taskflow_api_etl()
