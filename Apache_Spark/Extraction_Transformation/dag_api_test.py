import requests
import os

AIRFLOW_API_ENDPOINT =os.environ.get("AIRFLOW_API_ENDPOINT")
DAG_ID = "pyspark_with_params" # dag to trigger

# these are the custom parameters
parameters = {"parcel_number": "charge/200"}
print(parameters)



result = requests.post(f"{AIRFLOW_API_ENDPOINT}/dags/{DAG_ID}/dag_runs", json={"conf": parameters})
print(result)