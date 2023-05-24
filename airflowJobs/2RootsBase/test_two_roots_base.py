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
    'test_two_roots_base',
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

    def start1():
        print("start1")


    def start2():
        print("start2")


    with TaskGroup("start", tooltip="Start of DAG") as start:

        start_1 = PythonOperator(task_id='start_1', python_callable=start1)

        start_2 = PythonOperator(task_id='start_2', python_callable=start1)

    t2 = PythonOperator(task_id='second', python_callable=second)

    t3 = PythonOperator(task_id='end', python_callable=end)

start >> t2 >> t3
