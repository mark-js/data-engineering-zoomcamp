FROM apache/airflow:slim-3.2.0-python3.12

ARG jdk_version=17

USER root

RUN apt-get update && apt-get install -y \
    wget \
    && rm -rf /var/lib/apt/lists/*

RUN usermod -u 1000 airflow

USER airflow

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt