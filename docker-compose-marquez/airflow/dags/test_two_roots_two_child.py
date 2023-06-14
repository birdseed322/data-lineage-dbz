import random

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup

default_args = {
    'owner': 'Test',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'email': ['test@example.com']
}

with DAG(
    'test_two_roots_two_child',
    schedule_interval='*/1 * * * *',
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    default_args=default_args,
    description='DAG with two roots'
) as dag:


    def end():
        print("start")

    def second():
        print("second")



    with TaskGroup("start", tooltip="Start of DAG") as start:

        start_1 = TriggerDagRunOperator(
            task_id='trigger_test_two_roots_two_child_child1_dag',
            trigger_dag_id='test_two_roots_two_child_child1',
            conf={"message": "my_data"},
            wait_for_completion=True
        )

        start_2 = TriggerDagRunOperator(
            task_id='trigger_test_two_roots_two_child_child2_dag',
            trigger_dag_id='test_two_roots_two_child_child2',
            conf={"message": "my_data"},
            wait_for_completion=True
        )
    t2 = PythonOperator(task_id='second', python_callable=second)

    t3 = PythonOperator(task_id='end', python_callable=end)

start >> t2 >> t3
