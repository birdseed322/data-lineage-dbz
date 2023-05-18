from airflow.decorators import dag, task
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime

@dag(start_date=datetime(2023, 1, 1), schedule='@daily', catchup=False)
def waiting_for_a_b_c():

	waiting_for_a = ExternalTaskSensor(
		task_id='waiting_for_a',
		external_dag_id='dag_a',
		external_task_id='task_a'
	)

	waiting_for_b = ExternalTaskSensor(
		task_id='waiting_for_b',
		external_dag_id='dag_b',
		external_task_id='task_b'
	)

	waiting_for_c = ExternalTaskSensor(
		task_id='waiting_for_c',
		external_dag_id='dag_c',
		external_task_id='task_c'
	)

	@task
	def next_a():
		print("a")

	@task
	def next_b():
		print("b")

	@task
	def next_c():
		print("c")

	@task
	def done():
		print("done")

	waiting_for_a >> next_a() >> done()
	waiting_for_b >> next_b() >> done()
	waiting_for_c >> next_c() >> done()

waiting_for_a_b_c()