from datetime import datetime
from time import sleep

from airflow.decorators import dag, task

version = "0.1.25.5"


@dag(schedule_interval=None, start_date=datetime(2022, 1, 1), catchup=False, tags=['docker'])
def docker_dag():
    @task.docker(image='python:3.8-slim-buster')
    def run():
        tries = 10
        for i in range(tries):
            msg = f'loop {i}/{tries}  Hello world v.{version} time now: {datetime.now()}'
            print(msg)
            sleep(15)

    print(f"start")
    run()
    print(f"end")


dag = docker_dag()
