# Get the sentiment score from the mock API
def generate_sentiment_score(**kwargs):
    import random
    import logging
    import pandas as pd
    def mock_sentiment_api(text):
        sentiment_score = random.uniform(0, 1)
        return sentiment_score 
    
    ti = kwargs['ti']
    data=kwargs['ti'].xcom_pull(key='data', task_ids='clean_data')

    for i in range(0,len(data)):
        data[i]=pd.DataFrame(data[i])
        data[i]['sentiment_score'] = data[i].apply(mock_sentiment_api, axis=1)
        data[i]=data[i].to_dict()
    
    ti = kwargs['ti']
    kwargs['ti'].xcom_push(key='data', value=data)
    
    logger = logging.getLogger("airflow.task")
    task_instance = kwargs['ti']
    task_id = task_instance.task_id
    dag_id = task_instance.dag_id
    run_id = task_instance.run_id

    # Log task information
    logger.info(f"Running task _Test: {task_id}")
    logger.info(f"DAG ID: {dag_id}")
    logger.info(f"Run ID: {run_id}")

#print(generate_sentiment_score(data)[0])