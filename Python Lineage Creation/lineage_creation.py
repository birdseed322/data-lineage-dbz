
import requests
import base64
airflow_backend = "http://localhost:8080/api/v1/"
airflow_user = "airflow"
airflow_password = "airflow"


def test():
  headers =  {
      "Authorization": "Basic " + base64.b64encode(str.encode(f'{airflow_user}:{airflow_password}')).decode('utf-8'),
  }
  print(headers)
  response = requests.get(f"{airflow_backend}dags",headers=headers)
  print(response.json())
  print("done")


def start():
  print("start")

test()