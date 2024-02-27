FROM apache/airflow:latest as build

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends default-libmysqlclient-dev gcc && \
    rm -rf /var/lib/apt/lists/* 


USER airflow

RUN pip install --no-cache-dir mysqlclient

# Copy DAGs after installing dependencies
COPY . /opt/airflow/dagscd
