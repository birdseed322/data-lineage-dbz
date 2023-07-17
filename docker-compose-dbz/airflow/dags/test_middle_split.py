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
    'test_middle_split',
    schedule_interval='*/1 * * * *',
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    default_args=default_args,
    description='DAG that splits into 2 tasks in the middle'
) as dag:


    def end():
        print("end")

    def start():
        print("start")

    def middle1():
        print("middle1")


    def middle2():
        print("middle2")


    with TaskGroup("middle", tooltip="Middle of DAG") as middle:

        middle_1 = PythonOperator(task_id='middle_1', python_callable=middle1)

        middle_2 = PythonOperator(task_id='middle_2', python_callable=middle2)

    t1 = PythonOperator(task_id='start', python_callable=start)

    t3 = PythonOperator(task_id='end', python_callable=end)

t1 >> middle >> t3
