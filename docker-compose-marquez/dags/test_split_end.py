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
    'test_split_end',
    schedule_interval='*/1 * * * *',
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    default_args=default_args,
    description='DAG that triggers another DAG and ends with a split end'
) as dag:


    def start():
        print("start")


    def end1():
        print("end1")


    def end2():
        print("end2")


    with TaskGroup("end", tooltip="End of DAG") as end:

        end_1 = PythonOperator(task_id='end_1', python_callable=end1, dag=dag)

        end_2 = PythonOperator(task_id='end_2', python_callable=end2, dag=dag)

    t1 = PythonOperator(task_id='start', python_callable=start)

    trigger = TriggerDagRunOperator(
        task_id='trigger_test_split_end_target_dag',
        trigger_dag_id='test_split_end_target',
        conf={"message": "my_data"},
        wait_for_completion=True
    )

t1 >> trigger >> end
