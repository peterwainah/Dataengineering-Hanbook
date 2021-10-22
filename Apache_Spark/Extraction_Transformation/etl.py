##import spark session
import json

from pyspark.sql import SparkSession
from Apache_Spark.setting.project_config import *


## spark session

# SparkSession.builder.master(f"{os.environ.get('SPARK_MASTER')}")
spark=SparkSession.builder.master("local")\
    .appName("Geopro")\
    .config("spark.jars", os.environ.get('driver_path'))\
    .getOrCreate()

database_config=json.loads(os.environ.get('EDMS_V3'))

def etl_spark():
    """using subquery to read frrom postgres"""
    df = spark.read.jdbc(url = f"jdbc:postgresql://{database_config['host']}:{database_config['port']}/{database_config['database']}",

                         table = "(SELECT document_type, file_number, folio_number, metadata, document FROM nairobi_central.registration_main_test where file_number='charge/200') AS my_table",
                         properties={"user": f"{database_config['user']}", "password": f"{database_config['password']}", "driver": 'org.postgresql.Driver'})\
        # .createTempView('tbl')


    """using collect() function to loop through the dataframe"""

    ##Storing in variable
    data_collect=df.collect()

    # looping through each of the dataframe
    for values in data_collect:
        registration_dict={"document_type":values['document_type'],
                           "metadata":values['metadata']}
        print(registration_dict)


if __name__ == '__main__':
    etl_spark()