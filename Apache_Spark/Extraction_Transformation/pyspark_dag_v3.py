from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from pyspark.sql import SparkSession
from Apache_Spark.setting.project_config import *



spark=SparkSession.builder.master("local")\
    .appName("Geopro")\
    .config("spark.jars", os.environ.get('driver_path'))\
    .getOrCreate()

database_config=json.loads(os.environ.get('EDMS_V3'))


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




def etl_spark():
    """using subquery to read frrom postgres"""
    df = spark.read.jdbc(url = f"jdbc:postgresql://{database_config['host']}:{database_config['port']}/{database_config['database']}",

                         table = "(SELECT document_type, file_number, folio_number, metadata, document FROM nairobi_central.registration_main_test ) AS my_table",
                         properties={"user": f"{database_config['user']}", "password": f"{database_config['password']}", "driver": 'org.postgresql.Driver'})\
        # .createTempView('tbl')



    """using collect() function to loop through the dataframe"""

    ##Storing in variable
    file_number='charge/200'
    # data_collect=df.filter(df.file_number == f"'{file_number}'").collect()
    data_collect=df.filter(f"file_number == {file_number}").collect()
    # print(data_collect)


    # looping through each of the dataframe
    for values in data_collect:
        document_type=values['document_type']
        metadata=values['metadata']
        registration_dict={"document_type":values['document_type'],
                           "metadata":values['metadata']}
        print(values)

etl_spark()