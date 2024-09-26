FROM apache/airflow:2.4.2

RUN pip install pandas requests bs4 json lxml pymongo scipy numpy random

COPY dags/ /dags/