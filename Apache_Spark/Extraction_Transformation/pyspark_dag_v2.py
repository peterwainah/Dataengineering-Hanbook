
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args={
    'owner':'ADI',
    'depends_on_post':False,
    'email':['geoprodbteam@gmail.com'],
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':3,
    'retry_delay':timedelta(minutes=5)

}

dag=DAG(
    dag_id='pyspark_job_v2',
    default_args=default_args,
    start_date=days_ago(0),
    schedule_interval="2 * * * *",

)
pyspark_job_v2=BashOperator(
    task_id='pyspark_job_v2',
    bash_command="python3 /opt/Dataengineering-Handbook/Apache_Spark/main.py",
    dag=dag
)

pyspark_job_v2