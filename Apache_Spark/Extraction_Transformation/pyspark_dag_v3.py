from datetime import timedelta


from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from pyspark.sql import SparkSession
from Apache_Spark.setting.project_config import *




spark=SparkSession.builder.master("local")\
    .appName("Geopro")\
    .config("spark.jars", os.environ.get('driver_path'))\
    .getOrCreate()

database_config=json.loads(os.environ.get('EDMS_V3'))


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
    dag_id="pyspark_with_params",
    schedule_interval=None,
    start_date=days_ago(0)
)


"""arguments to be triggered manualy"""

dag.trigger_arguments={"parcel_number":"string"}
def pass_parcel(**kwargs):

    """getting parameters specified during dag trigger"""
    dag_run_conf=kwargs['dag_run'].conf
    kwargs["ti"].xcom_push(key="parcel_number",value=dag_run_conf["parcel_number"]) ##push it as airflow xcom







def etl_spark(**kwargs):
    # pass_parcel
    # parcel_no=kwargs['parcel_number']

    ti=kwargs['ti']
    parcel_no = ti.xcom_pull(key='parcel_number',task_ids='pass_parcel')
    # # print(parcel_no)


    """using subquery to read from postgres"""
    df = spark.read.jdbc(url = f"jdbc:postgresql://{database_config['host']}:{database_config['port']}/{database_config['database']}",

                         table = "(SELECT document_type, file_number, folio_number, metadata, document FROM nairobi_central.registration_main_test ) AS my_table",
                         properties={"user": f"{database_config['user']}", "password": f"{database_config['password']}", "driver": 'org.postgresql.Driver'})\
        # .createTempView('tbl')



    """using collect() function to loop through the dataframe"""

    ##Storing in variable

    # data_collect=df.filter(df.file_number == f"'{file_number}'").collect()
    data_collect=df.filter(f"file_number == '{parcel_no}'").collect()
    # data_collect = df.filter(df.file_number == 'charge/200').collect()
    # print(data_collect)


    # looping through each of the dataframe
    for values in data_collect:
        document_type=values['document_type']
        metadata=values['metadata']
        registration_dict={"document_type":values['document_type'],
                           "metadata":values['metadata']}
        print(values)



parcel_parameter=PythonOperator(
    task_id="pass_parcel",
    python_callable=pass_parcel,
    provide_context=True,
    dag=dag
)

spark_task=PythonOperator(
    task_id="etl",
    python_callable=etl_spark,
    provide_context=True,
    dag=dag
)

parcel_parameter >> spark_task
