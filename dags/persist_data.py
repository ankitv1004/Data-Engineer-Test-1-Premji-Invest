def persist_data(**kwargs):
    from pymongo import MongoClient
    import pandas as pd
    import logging
    # MongoDB Atlas connection string with srv
    connection_string = "mongodb+srv://ankitchail:Jm8K9C54gxSWxqso@cluster0.u41jx.mongodb.net/"

    # Connect to MongoDB Atlas
    client = MongoClient(connection_string)

    #pull summary data from previous task
    ti = kwargs['ti']
    data=kwargs['ti'].xcom_pull(key='data', task_ids='generate_sentiment_score')
    
    #data = pd.DataFrame(data)
    
    for i in range(0,len(data)):
        # Access the database and collection
        db = client['persist_db'] 
        keywords=["HDFC","Tata Motars"]
        conn = db[f"{keywords[i]}"]  
        data[i]=pd.DataFrame(data[i]) 
        data[i]=data[i].to_dict('records')
        conn.insert_many(data[i])

    logger = logging.getLogger("airflow.task")
    task_instance = kwargs['ti']
    task_id = task_instance.task_id
    dag_id = task_instance.dag_id
    run_id = task_instance.run_id

    # Log task information
    logger.info(f"Running task _Test: {task_id}")
    logger.info(f"DAG ID: {dag_id}")
    logger.info(f"Run ID: {run_id}")
    print("Data inserted successfully.")

