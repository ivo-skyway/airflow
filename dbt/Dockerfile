FROM python:3.8-slim-buster

RUN pip install --no-cache-dir dbt-postgres dbt-redshift

RUN mkdir /app
WORKDIR /app
COPY __main__.py .

CMD ["python3", "__main__.py"]
