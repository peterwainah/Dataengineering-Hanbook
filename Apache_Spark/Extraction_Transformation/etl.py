##import spark session
import json

from pyspark.sql import SparkSession
from Apache_Spark.setting.project_config import *


## spark session
driver_path = "/home/peterwainaina/ETL_Project/resources/drivers/postgresql-42.2.23.jar"
spark=SparkSession.builder.master(f"{os.environ.get('SPARK_MASTER')}")\
    .appName("Geopro")\
    .config("spark.jars", driver_path)\
    .getOrCreate()

database_config=json.loads(os.environ.get('EDMS_V3'))


"""using subquery to read frrom postgres"""
df = spark.read.jdbc(url = f"jdbc:postgresql://{database_config['host']}:{database_config['port']}/{database_config['database']}",

                     table = "(SELECT document_type, file_number, folio_number, metadata, document FROM nairobi_central.registration_main_test) AS my_table",
                     properties={"user": f"{database_config['user']}", "password": f"{database_config['password']}", "driver": 'org.postgresql.Driver'})
print(df)