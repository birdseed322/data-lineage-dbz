from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
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
    'test_db_four',
    schedule_interval='*/3 * * * *',
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    default_args=default_args,
    description='DAG that pulls from one postgres db_one.'
)

def start():
    print("start")

t1 = PythonOperator(task_id='start', python_callable=start, dag=dag)

t2 = PostgresOperator(
    task_id='if_four_not_exists',
    postgres_conn_id='postgres_default',
    sql='''
    CREATE TABLE IF NOT EXISTS db_four (
      value INTEGER
    );''',
    dag=dag
)

t3 = PostgresOperator(
    task_id='agg_four',
    postgres_conn_id='postgres_default',
    sql='''
    INSERT INTO db_four (value)
        SELECT SUM(c.value) FROM db_one AS c;
    ''',
    dag=dag
)

t1 >> t2 >> t3