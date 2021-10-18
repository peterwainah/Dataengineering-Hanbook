
from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
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
    dag_id='transactable_delta',
    default_args=default_args,
    start_date=days_ago(0),
    schedule_interval="2 * * * *",

)

spark_config={
    'conn_id':'spark_local',
    'application':'/opt/Dataengineering-Handbook/Apache_Spark/main.py'

}
pyspark_job=SparkSubmitOperator(task_id="pyspark_task",
                           **spark_config,
                            dag=dag)
pyspark_job