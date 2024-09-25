from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
import pandas as pd
from fetch_data import fetch_data
from clean_data import clean_data
from sentiment_score import generate_sentiment_score
from persist_data import persist_data



# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 9, 25),
    'retries': 0,
}
#DAG
dag = DAG(
    'pipeline1_dag',
    default_args=default_args,
    schedule_interval=timedelta(hours=7),  # Set the schedule as needed
    catchup=False,
)
#Task for fetch_data
fetch_data = PythonOperator(
    task_id='fetch_data',
    python_callable=fetch_data,
    provide_context=True,
    dag=dag,
)

#Task for Data cleaning
clean_data = PythonOperator(
    task_id='clean_data',
    python_callable=clean_data,
    provide_context=True,
    dag=dag,
)

#Task for generate_sentiment_score
generate_sentiment_score = PythonOperator(
    task_id='generate_sentiment_score',
    python_callable=generate_sentiment_score,
    provide_context=True,
    dag=dag,
)

#Task for inserting data in Mongodb
persist_data = PythonOperator(
    task_id='persist_data',
    python_callable=persist_data,
    provide_context=True,
    dag=dag,
)

fetch_data>>clean_data>>generate_sentiment_score>>persist_data