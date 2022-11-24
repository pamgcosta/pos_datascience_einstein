import os, json
import pandas as pd
import logging

from datetime import datetime


from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


# pip install google-api-python-client
from googleapiclient import discovery
# pip install google
from google.oauth2.service_account import Credentials

os.chdir('/usr/local/airflow/dags')

def get_credential():
    
    credentials = Credentials.from_service_account_file(
        'cred.json',
        scopes=[u'https://www.googleapis.com/auth/bigquery']
    )

    return credentials

def transform():
    logging.info(os.getcwd())
    df = pd.read_csv('data.csv')

    df = df[df['sample'].notna()]
    df = df[df['dt_birth'].notna()]
    df["age"] = df["age"].astype(int)

    df.to_csv('transformed_data.csv', index=False)

    return ''

def send_to_gcp():

    df = pd.read_csv('transformed_data.csv')


    df.to_gbq(
        destination_table="estudo_de_caso.sorologia", 
        project_id="aerobic-datum-330818", 
        if_exists='replace',
        credentials=get_credential(), 
        table_schema=json.loads(open('data_schema.json').read()) 
    )
    
    return ''

dag = DAG('estudo_de_caso', description='',
          schedule_interval='0 12 * * *',
          start_date=datetime(2021, 11, 1), catchup=False)


transform_op = PythonOperator(
    task_id='transform', 
    python_callable=transform, 
    dag=dag
)


send_to_gcp_op = PythonOperator(
    task_id='send_to_gcp', 
    python_callable=send_to_gcp, 
    dag=dag
)

dag >> transform_op >> send_to_gcp_op