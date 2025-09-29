FROM apache/airflow:2.7.3

USER root
RUN apt-get update && apt-get install -y unzip && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir kaggle

RUN pip install --no-cache-dir dbt-bigquery==1.8.2

RUN mkdir -p /home/airflow/.kaggle && chmod 700 /home/airflow/.kaggle
