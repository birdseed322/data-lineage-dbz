from airflow.decorators import dag, task
from datetime import datetime
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


@dag(start_date=datetime(2023, 1, 1), schedule='@daily', catchup=False)
def test_wait_target1():

    @task
    def start(dag_run=None):
        print(dag_run.conf.get("message"))
        return "Trigger DAG"

    trigger = TriggerDagRunOperator(
        task_id='trigger_test_wait_target2_dag',
        trigger_dag_id='test_wait_target2',
        conf={"message": "my_data"},
        wait_for_completion=True
    )

    @task
    def done():
        print("done")
        
    start() >> trigger >> done()

test_wait_target1()
