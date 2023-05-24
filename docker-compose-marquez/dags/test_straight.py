import random

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'Test',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'email': ['test@example.com']
}

dag = DAG(
    'test_straight',
    schedule_interval='*/1 * * * *',
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    default_args=default_args,
    description='DAG that executes a linear sequence of tasks'
)

def start():
    print("start")
    
def second():
    print("second")
    
def third():
    print("third")

def done():
    print("done")

t1 = PythonOperator(task_id='start', python_callable=start, dag=dag)

t2 = PythonOperator(task_id='second', python_callable=second, dag=dag)

t3 = PythonOperator(task_id='third', python_callable=third, dag=dag)

t4 = PythonOperator(task_id='done', python_callable=done, dag=dag)

t1 >> t2 >> t3 >> t4