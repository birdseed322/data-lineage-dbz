import random

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'dbz',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'email': ['dbz@example.com']
}

dag = DAG(
    'test_db_two',
    schedule_interval='*/1 * * * *',
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    default_args=default_args,
    description='DAG that generates a count value'
)

t1 = PostgresOperator(
    task_id='if_two_not_exists',
    postgres_conn_id='postgres_default',
    sql='''
    CREATE TABLE IF NOT EXISTS db_two (
      value INTEGER
    );''',
    dag=dag
)

t2 = PostgresOperator(
    task_id='inc_two',
    postgres_conn_id='postgres_default',
    sql='''
    INSERT INTO db_two (value)
         VALUES (1)
    ''',
    dag=dag
)

t1 >> t2