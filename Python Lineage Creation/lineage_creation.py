import requests
import base64
airflow_backend = "http://localhost:8080/api/v1/"
airflow_user = "airflow"
airflow_password = "airflow"
dictionary = {}
headers =  {
  "Authorization": "Basic " + base64.b64encode(str.encode(f'{airflow_user}:{airflow_password}')).decode('utf-8'),
  "Content-type": "application/json"
}

def get_dag_ids(**context):
  response = requests.get(f"{airflow_backend}dags",headers=headers)
  if response.status_code == 200:
    data = response.json()
    for dag in data["dags"]:
      key = dag["dag_id"]
      value = False
      dictionary[key] = value
    context['ti'].xcom_push(key='dictionary', value=dictionary)
    for key in dictionary:
      if dictionary[key] == False:
        dummy_recursive_function(key)
    print(dictionary)
  else:
     print(f"Failed to fetch DAGs with error code {response.status_code}")

def dummy_recursive_function(dag_id, **context):
  dictionary = context['ti'].xcom.pull(key='dictionary')
  dictionary[dag_id] = True
  response = requests.get(f"{airflow_backend}dags/{dag_id}",headers=headers)
  data = response.json()

# def create_lineage():
#   for key in dictionary:
#     if dictionary[key] == False:
#       dummy_recursive_function(key)

get_dag_ids()
# create_lineage()
# dummy_recursive_function("lineage_creation")