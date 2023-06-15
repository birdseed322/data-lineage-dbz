from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

@dag(start_date=datetime(2023, 1, 1), schedule='@daily', catchup=False)
def controller():

	@task
	def start():
		return "Controller DAG start"

	trigger = TriggerDagRunOperator(
		task_id='trigger_target_dag',
		trigger_dag_id='target',
		conf={"message": "my_data"},
		wait_for_completion=True
	)

	@task
	def done():
		print("done")

	start() >> trigger >> done()

controller()