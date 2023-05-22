from airflow.decorators import dag, task
from datetime import datetime

@dag(start_date=datetime(2023, 1, 1), schedule='@daily', catchup=False)
def dag_b():

	@task
	def task_b():
		print("task_b")

dag_b()