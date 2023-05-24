from airflow.decorators import dag, task
from datetime import datetime


@dag(start_date=datetime(2023, 1, 1), schedule='@daily', catchup=False)
def test_wait_target2():

    @task
    def start(dag_run=None):
        print(dag_run.conf.get("message"))
        return "Trigger DAG"

    @task
    def done():
        print("done")
    
    start() >> done()

test_wait_target2()
