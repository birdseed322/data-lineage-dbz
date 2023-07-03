from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator

import requests
import base64
airflow_backend = "http://airflow:8080/api/v1/"
airflow_user = "airflow"
airflow_password = "airflow"
headers =  {
  "Authorization": "Basic " + base64.b64encode(str.encode(f'{airflow_user}:{airflow_password}')).decode('utf-8'),
  "Content-type": "application/json"
}
default_args = {
    'owner': 'dbz',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'email': ['datascience@example.com']
}

with DAG(
    'lineage_creation',
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    default_args=default_args,
    description='DAG that generates lineage creation of all other DAGs in Neo4j.'
) as dag:

    def test():
      response = requests.get(f"{airflow_backend}dags",headers=headers)

    t1 = PythonOperator(task_id='test', python_callable=test)

      # if response.status_code == 200:
      # data = response.json()
      # dictionary = {}
      # for dag in data["dags"]:
      #   key = dag["dag_id"]
      #   value = False
      #   dictionary[key] = value
      
      # print(dictionary)

    def start():
      print("start")

    t2 = PythonOperator(task_id='start', python_callable=start)

    t1 >> t2