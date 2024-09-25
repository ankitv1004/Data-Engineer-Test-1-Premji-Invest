def clean_data(**kwargs):
    import logging
    import pandas as pd
    ti = kwargs['ti']
    data=kwargs['ti'].xcom_pull(key='data', task_ids='fetch_data')

    for i in range(0,len(data)):
        # Sorting by column "published_date"
        data[i]=pd.DataFrame(data[i])
        data[i]=data[i].sort_values(by=['published_date'], ascending=False)
        data[i]=data[i].iloc[0:5,:]
        data[i]=data[i].to_dict()
        #print(data[i])

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

#print(clean_data(data))