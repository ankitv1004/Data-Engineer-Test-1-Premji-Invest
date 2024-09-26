FROM apache/airflow:2.4.2
ENV AIRFLOW_VERSION=2.4.2
RUN pip install apache-airflow==${AIRFLOW_VERSION} pandas requests bs4 lxml pymongo 
RUN pip install apache-airflow==${AIRFLOW_VERSION} scipy numpy

COPY dags/ /dags/