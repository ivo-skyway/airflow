from datetime import datetime
from time import sleep

now = datetime.now()
start = now
version = "0.1.27.3"


def print_hello():
    tries = 10

    with open("/tmp/mylog", "wt") as f:
        for i in range(tries):
            msg = f'loop {i + 1}/{tries}  Hello world v.{version} time now: {datetime.now()}'
            f.write(msg + "\n")
            f.flush()
            print(msg)
            sleep(10)

    return "OK"


if __name__ == "__main__":
    print_hello()
