## importing aiflow libraries

from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import json


### define dag arguments
default_args = {
    'owner': 'waina',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

def extract():
    data_string = '{"1001": 301.27, "1002": 433.21, "1003": 502.22}'
    return json.loads(data_string)

def transform(order_data):
    print(type(order_data))
    for value in order_data.values():
        total_order_value = value
    return {"total_order_value": "total_order_value"}




## define dag
dag = DAG(
    dag_id="etldag",
    schedule_interval=None,
    start_date=days_ago(2),
    default_args=default_args,
    schedule_interval="* 2 * * *",
)

# define second task

extract_task = PythonOperator(
    task_id="extract",
    python_callable=extract,
    op_kwargs={"x":"Apache Airflow"},
    dag=dag,
)
## define first task

transform_task = PythonOperator(
    task_id="transform",
    op_kwargs={"order_data": "{{ti.xcom_pull('extract')}}"},
    python_callable=transform,
    dag=dag,
)

extract_task >> transform_task