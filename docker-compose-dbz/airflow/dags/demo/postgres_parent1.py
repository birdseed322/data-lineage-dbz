from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
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

with DAG(
    "postgres_parent1",
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    default_args=default_args,
    description="DAG that trigger external Postgres interacting DAGs",
) as dag:

    create_db_1 = PostgresOperator(
        task_id='if_one_not_exists',
        postgres_conn_id='postgres_default',
        sql='''
        CREATE TABLE IF NOT EXISTS db_one (
            value INTEGER
        );
         ''',
        dag=dag
    )

    create_db_2 = PostgresOperator(
        task_id='if_two_not_exists',
        postgres_conn_id='postgres_default',
        sql='''
        CREATE TABLE IF NOT EXISTS db_two (
            value INTEGER
        );
         ''',
        dag=dag
    )
    
    insert_db_1 = PostgresOperator(
        task_id='insert_into_one',
        postgres_conn_id='postgres_default',
        sql='''
        INSERT INTO db_one (value)
            VALUES (1);
         ''',
        dag=dag
    )
    
    insert_db_2 = PostgresOperator(
        task_id='insert_into_two',
        postgres_conn_id='postgres_default',
        sql='''
        INSERT INTO db_two (value)
            VALUES (2);
         ''',
        dag=dag
    )

    trigger_child = TriggerDagRunOperator(
        task_id='test_trigger_child',
        trigger_dag_id='postgres_child1',
        wait_for_completion=True
    )

    create_db_1 >> create_db_2 >> insert_db_1 >> insert_db_2 >> trigger_child

