# import libraries
from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import os
#from airflow.hooks import PostgresHook
import json
import numpy as np
from airflow.utils.dates import days_ago




def load_data():
    """Process the json data,checks the types and enters into postgres database"""
    #pg_hook =PostgresHook(postgres_conn_id)='weather_id'


# Define default dag arguments
default_args={
    'owner':'waina',
    'depends_on_past':False,
    'email':['petwah17@gmail.com'],
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':5,
    'retry_delay':timedelta(minutes=1)
}
# Define the dag ,the start date and how frequently it runs
## This dag will run after every 2 hours

dag=DAG(
    dag_id='weatherDag',
    default_args=default_args,
    start_date=days_ago(2),
    schedule_interval="* */2 * * *"
)

## First task it to query get the weather from openweathermap
task1=BashOperator(
    task_id='get_weather',
    bash_command='python ~airflow/dags/src/getweather.py',
    dag=dag
)

## second task is to process the data and load into the database.
task2 = PythonOperator(
    task_id='tranform_load',
    python_callable=load_data,
    dag=dag
)