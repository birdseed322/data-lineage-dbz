from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from datetime import datetime
default_args = {
    'owner': 'dbz',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'email': ['dbz@example.com']
}


@dag(schedule_interval='*/1 * * * *', catchup=False, default_args=default_args, description="DAG that trigger external Postgres interacting DAGs")
def test_master_dag_db():

    trigger1 = TriggerDagRunOperator(
        task_id='trigger_test_db_two',
        trigger_dag_id='test_db_two',
        wait_for_completion=True
    )

    trigger2 = TriggerDagRunOperator(
        task_id='trigger_test_db_three',
        trigger_dag_id='test_db_three',
        wait_for_completion=True
    )
    trigger3 = TriggerDagRunOperator(
        task_id='trigger_test_two_postgres',
        trigger_dag_id='test_two_postgres',
        wait_for_completion=True
    )
    trigger4 = TriggerDagRunOperator(
        task_id='trigger_test_db_four',
        trigger_dag_id='test_db_four',
        wait_for_completion=True
    )
    
    trigger1 >> trigger2 >> trigger3 >> trigger4


test_master_dag_db()
