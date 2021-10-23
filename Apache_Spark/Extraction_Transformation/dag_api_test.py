import requests

AIRFLOW_API_ENDPOINT ="http://192.168.214.210:8081/api/experimental/dags/pyspark_with_params/dag_runs"
DAG_ID = "pyspark_with_params" # dag to trigger

# these are the custom parameters
parameters = {"parcel_number": "ACME"}

result = requests.post(AIRFLOW_API_ENDPOINT,json={"conf": parameters})
                       # json={"conf": parameters})
print(result)