from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "Test",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "email": ["test@example.com"],
}

with DAG(
    "demo_dag",
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    default_args=default_args,
    description="Demonstration DAG that showcases the different operators and how it is captured on Neo4J",
) as dag:
    def start():
      print("Start of demonstration Dag")

    t1 = PythonOperator(task_id="start", python_callable=start)

    t2 = TriggerDagRunOperator(
        task_id="trigger_spark_jobs",
        trigger_dag_id="test_spark_jobs",
        wait_for_completion=False,
    )

    t3 = TriggerDagRunOperator(
        task_id="trigger_postgres",
        trigger_dag_id="test_postgres_parent",
        wait_for_completion=True,
    )

    t1 >> t2 >> t3