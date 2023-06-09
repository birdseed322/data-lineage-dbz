from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from datetime import datetime

default_args = {
    "owner": "Test",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "email": ["test@example.com"],
}


@dag(
    schedule_interval="*/1 * * * *",
    catchup=False,
    default_args=default_args,
    description="DAG that trigger external DAG without waiting for the triggered DAG to finish",
)
def test_no_wait():
    @task
    def start():
        return "Controller DAG start"

    trigger = TriggerDagRunOperator(
        task_id="trigger_test_target1_dag",
        trigger_dag_id="test_no_wait_target1",
        conf={"message": "my_data"},
        wait_for_completion=False,
    )

    @task
    def done():
        print("done")

    start() >> trigger >> done()


test_no_wait()
