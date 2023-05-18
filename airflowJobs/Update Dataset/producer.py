from airflow.decorators import dag, task
from datetime import datetime
from airflow.datasets import Dataset

data_a = Dataset("/home/wesley/marquez/examples/airflow/data_dictionary.csv")
data_b = Dataset("/home/wesley/marquez/examples/airflow/resorts.csv")

@dag(start_date=datetime(2023, 1 ,1), schedule='@daily', catchup=False)
def producer():

	@task(outlets=[data_a])
	def update_a():
		print("update A") 

	@task(outlets=[data_b])
	def update_b():
		print("update B")

	update_a() >> update_b()

producer()