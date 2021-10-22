from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago


dag=DAG(
    dag_id="pyspark_with_params",
    schedule_interval=None,
)
"""arguments to be triggered manualy"""
dag.trigger_arguments={"parcel_number":"string"}

def pass_param(**kwargs):
    """getting parameters specified during dag trigger"""
    dag_run_conf=kwargs["dag_run"].conf
    kwargs["ti"].xcom_push(key="parcel_number",value=dag_run_conf["parcel_number"]) ##push it as airflow xcom