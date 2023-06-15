from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'Ezekiel',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'email': ['ezekiel@example.com']
}

dag = DAG(
    'test',
    schedule_interval='*/1 * * * *',
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    default_args=default_args,
    description='test'
)

t1 = PostgresOperator(
    task_id='test',
    postgres_conn_id='postgres_default',
    sql='''
    CREATE TABLE IF NOT EXISTS test (
      test INTEGER
    );''',
    dag=dag
)

t1