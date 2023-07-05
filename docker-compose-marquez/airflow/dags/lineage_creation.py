from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.dates import days_ago
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
    'email': ['datascience@example.com'],
}

with DAG(
    'lineage_creation',
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    default_args=default_args,
    description='DAG that generates lineage creation of all other DAGs in Neo4j.'
) as dag:

    def get_dag_ids(**context):
      try: 
        dictionary={}
        response = requests.get(f"{airflow_backend}dags",headers=headers)
        data = response.json()
        for dag in data["dags"]:
          key = dag["dag_id"]
          dictionary[key] = False
        context['ti'].xcom_push(key='dictionary', value=dictionary)
        for key in dictionary:
          if dictionary[key] == False:
            print("Go through function")
            dummy_recursive_function(key, **context)
          else:
            print("skipped DAG")
        dictionary = context['ti'].xcom_pull(key='dictionary')
        print(dictionary) 
      except requests.exceptions.RequestException as err:
        print("Error occurred during DAGs retrieval:", err)

    get_dag_ids = PythonOperator(task_id='get_dag_ids', python_callable=get_dag_ids)

    def dummy_recursive_function(dag_id, **context):
      try:
        dictionary = context['ti'].xcom_pull(key='dictionary')
        dictionary[dag_id] = True
        context['ti'].xcom_push(key='dictionary', value=dictionary)
        response = requests.get(f"{airflow_backend}dags/{dag_id}/tasks",headers=headers)
        data = response.json()
        for task in data.tasks:
          
      except requests.exceptions.RequestException as err:
        print("Error occurred during lineage creation", err)     

    get_dag_ids