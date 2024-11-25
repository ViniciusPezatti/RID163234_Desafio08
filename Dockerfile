FROM apache/airflow:2.3.0

USER root
RUN pip install pandas
USER airflow