FROM python:3.8-slim-buster

RUN pip install --no-cache-dir dbt-postgres~=2.0.0 dbt-redshift~=2.0.0; \
    mkdir /app

WORKDIR /app
COPY __main__.py .

ENV PYTHONUNBUFFERED=1

CMD ["python3", "__main__.py"]
