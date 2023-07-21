import random

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

simple_spark_job_path = "/usr/local/spark/app/simple_spark_job.py"
complex_spark_job_path = "/usr/local/spark/app/complex_spark_job.py"
spark_master = "spark://spark:7077"

default_args = {
    "owner": "Test",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "email": ["test@example.com"],
}

with DAG(
    "spark_parent1",
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    default_args=default_args,
    description="DAG that triggers a complex SPARK job",
) as dag:

    def start():
        print("start")

    t1 = PythonOperator(task_id="start", python_callable=start)

# t2 Creates the necessary tables for the upcoming Spark jobs
    t2 = PostgresOperator(
        task_id="create_tables",
        postgres_conn_id="postgres_default",
        sql="""
    CREATE TABLE IF NOT EXISTS visits (
        user_id integer,
        visit_date date
    );
    INSERT INTO visits (user_id, visit_date) VALUES
        (1, '2023-01-01'),
        (2, '2023-01-02'),
        (1, '2023-01-03'),
        (3, '2023-01-04'),
        (1, '2023-01-05'),
        (1, '2023-01-06'),
        (2, '2023-01-07'),
        (4, '2023-01-08'),
        (1, '2023-01-09'),
        (1, '2023-01-10'),
        (2, '2023-01-11')
    ON CONFLICT DO NOTHING;

    CREATE TABLE IF NOT EXISTS page_views (
        page_id integer,
        duration integer
    );
    INSERT INTO page_views (page_id, duration)
    VALUES
        (101, 10),
        (102, 15),
        (103, 8),
        (104, 20),
        (105, 12),
        (106, 18),
        (107, 9),
        (108, 25),
        (109, 14),
        (110, 11)
    ON CONFLICT DO NOTHING;

    CREATE TABLE IF NOT EXISTS page_categories (
        page_id integer,
        category text
    );
    INSERT INTO page_categories (page_id, category) 
    VALUES
        (101, 'Technology'),
        (102, 'Sports'),
        (103, 'Technology'),
        (104, 'Entertainment'),
        (105, 'Technology'),
        (106, 'Sports'),
        (107, 'Technology'),
        (108, 'Entertainment'),
        (109, 'Technology'),
        (110, 'Sports')
    ON CONFLICT DO NOTHING;
    """,
        dag=dag,
    )

    t3 = SparkSubmitOperator(
        application=simple_spark_job_path,
        task_id="submit_spark_job",
        name="simple_spark_job",
        conn_id="spark_default",
        packages="io.openlineage:openlineage-spark:0.27.2",
        conf={
            "spark.openlineage.transport.url": "http://marquez:5000/api/v1/namespaces/example/",
            "spark.openlineage.transport.type": "http",
            "spark.extraListeners": "io.openlineage.spark.agent.OpenLineageSparkListener",
        },
    )
    '''
    SELECT user_favorites.id, CONCAT(favorite_color, ' ', state) AS nickname
    FROM user_favorites
    JOIN locations
    ON user_favorites.favorite_city = locations.city
    '''

t1 >> t2 >> t3
