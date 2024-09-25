# Function to fetch and parse the data, return a list of dataframes for each keyword

def fetch_data(**kwargs):
    import json
    from bs4 import BeautifulSoup
    import pandas as pd
    import requests
    import logging
    
    df=pd.DataFrame(columns=['title','post_url','published_date','article'])
    # URLs to scrape
    keywords=["HDFC","Tata Motars"]
    # Fetch data from both URLs
    data=[]

    for keyword in keywords:
        url = f"https://backend.finshots.in/backend/search/?q={keyword}"
        
        # Fetch the webpage content
        response = requests.get(url)
        response.raise_for_status()  # Check for request errors
        #print(response.text)

        # Parse the HTML content
        
        soup = BeautifulSoup(response.content, 'html.parser')
        soup=json.loads(response.content)
        matches=soup["matches"]
        #print(matches[0])

        for match in matches:
            # Example: Get the title of the page
            title = match['title']
            #print(title)
            
            # url = match['post_url']
            post_url=match['post_url']
            published_date=match['published_date']
            req = requests.get(url=post_url)
            soup = BeautifulSoup(req.content, 'lxml')
            # Example: Get the first paragraph or some other content
            paragraph = soup.find('div',{"class":"post-content"}).text.strip()#.get_text() 
            #paragraph=soup.text.strip()
            row=pd.DataFrame({ "title": [title],"post_url": [post_url],'published_date':[published_date], "article": [paragraph]})
            #print(row)
            df=df.append(row,ignore_index=True)
        data.append(df.to_dict())

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

#print(fetch_data())