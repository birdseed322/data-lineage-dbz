import random

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

default_spark_job_path = '/usr/local/spark/app/my_spark_job.py'
spark_master = "spark://spark:7077"

default_args = {
    'owner': 'Test',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'email': ['test@example.com']
}

with DAG(
    'test_spark_submit',
    schedule_interval='*/1 * * * *',
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    default_args=default_args,
    description='DAG that triggers SPARK job'
) as dag:


    def start():
        print("start")

    t1 = PythonOperator(task_id='start', python_callable=start)
    
    t2 =  SparkSubmitOperator(application=default_spark_job_path, task_id="submit_job", conn_id='spark_default', packages='za.co.absa.spline.agent.spark:spark-3.2-spline-agent-bundle_2.12:1.1.0', conf={"spark.sql.queryExecutionListeners":"za.co.absa.spline.harvester.listener.SplineQueryExecutionListener", "spark.spline.producer.url":"http://rest-server:8080/producer"})

t1 >> t2
