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
    'test_two_postgres',
    schedule_interval='*/2 * * * *',
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    default_args=default_args,
    description='DAG that has 2 jobs that pulls from postgres.'
)

def start():
    print("start")

def done():
    print("done")

t1 = PythonOperator(task_id='start', python_callable=start, dag=dag)

t2 = PostgresOperator(
    task_id='if_one_not_exists',
    postgres_conn_id='postgres_default',
    sql='''
    CREATE TABLE IF NOT EXISTS db_one (
      value INTEGER
    );''',
    dag=dag
)

t3 = PostgresOperator(
    task_id='agg_one',
    postgres_conn_id='postgres_default',
    sql='''
    INSERT INTO db_one (value)
        SELECT SUM(c.value) FROM db_two AS c;
    ''',
    dag=dag
)

t4 = PostgresOperator(
    task_id='max',
    postgres_conn_id='postgres_default',
    sql='''
    INSERT INTO db_one (value)
        SELECT MAX(c.value) FROM db_three AS c;
    ''',
    dag=dag
)

t5 = PythonOperator(task_id='done', python_callable=done, dag=dag)

t1 >> t2 >> t3 >> t4 >> t5