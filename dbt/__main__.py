import json
from datetime import datetime
from time import sleep

now = datetime.now()
start = now
version = "0.1.28.0"


def print_hello():
    tries = 5

    for i in range(tries):
        msg = f'loop {i + 1}/{tries}  Hello world v.{version} time now: {datetime.now()}'
        print(msg)
        sleep(10)

    return {"number": tries, "status": "OK"}


def xcom_push(msg):
    with open("/airflow/xcom/return.json", "w") as file:
        json.dump(msg, file)


if __name__ == "__main__":
    ret = print_hello()
    xcom_push(ret)
