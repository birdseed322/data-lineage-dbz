from airflow.decorators import dag, task
from datetime import datetime
from airflow.datasets import Dataset

data_a = Dataset("/home/wesley/marquez/examples/airflow/data_dictionary.csv")
data_b = Dataset("/home/wesley/marquez/examples/airflow/resorts.csv")

@dag(start_date=datetime(2023, 1 ,1), schedule=[data_a, data_b], catchup=False)
def consumer():

	@task
	def run():
		print('run')

	run()

consumer()