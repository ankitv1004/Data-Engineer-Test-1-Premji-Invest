from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
import pandas as pd
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from pipeline2_mean_age import mean_age_by_occupation
from pipeline2_top_20_movies import top_20_movies
from pipeline2_top_genres import top_genres
from pipeline_2_similiar_movies import similiar_movies

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 9, 25),
    'retries': 0,
}
#DAG
dag = DAG(
    'pipeline2_dag',
    default_args=default_args,
    schedule_interval=timedelta(hours=7),  # Set the schedule as needed
    catchup=False,
)
def log_execution_date(execution_date):
    logging.info(f'Checking for completion of task on execution date: {execution_date}')
    return execution_date

wait_for_pipeline1 = ExternalTaskSensor(
    task_id='wait_for_pipeline1',
    external_dag_id='pipeline1_dag',  # The ID of Pipeline1
    external_task_id='persist_data',  # The ID of the last task in Pipeline1
    # execution_date_fn=log_execution_date,  # Ensure it runs for the same day
    timeout=300,  # Adjust this to fit your needs
    poke_interval=30,
    dag=dag,
)

#Task for mean_age_by_occupation
mean_age_by_occupation = PythonOperator(
    task_id='mean_age_by_occupation',
    python_callable=mean_age_by_occupation,
    provide_context=True,
    dag=dag,
)

#Task for top_20_movies
top_20_movies = PythonOperator(
    task_id='top_20_movies',
    python_callable=top_20_movies,
    provide_context=True,
    dag=dag,
)

#Task for top_genres
top_genres = PythonOperator(
    task_id='top_genres',
    python_callable=top_genres,
    provide_context=True,
    dag=dag,
)

#Task for similiar_movies
similiar_movies = PythonOperator(
    task_id='similiar_movies',
    python_callable=similiar_movies,
    provide_context=True,
    dag=dag,
)

#wait_for_pipeline1>>
mean_age_by_occupation>>top_20_movies>>top_genres>>similiar_movies