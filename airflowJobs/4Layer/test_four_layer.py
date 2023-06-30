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
    'test_four_layer',
    schedule_interval='*/5 * * * *',
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    default_args=default_args,
    description='DAG that has four levels'
)

t0 = PostgresOperator(
    task_id='L0_task_0',
    postgres_conn_id='postgres_default',
    sql='''
CREATE TABLE IF NOT EXISTS L0_t1 (
  c1 INTEGER,
  c2 INTEGER,
  c3 INTEGER
);
CREATE TABLE IF NOT EXISTS L0_t2 (
  c1 INTEGER,
  c2 INTEGER,
  c3 INTEGER
);
CREATE TABLE IF NOT EXISTS L0_t3 (
  c1 INTEGER,
  c2 INTEGER,
  c3 INTEGER
);
CREATE TABLE IF NOT EXISTS L0_t4 (
  c1 INTEGER,
  c2 INTEGER,
  c3 INTEGER
);
CREATE TABLE IF NOT EXISTS L0_t5 (
  c1 INTEGER,
  c2 INTEGER,
  c3 INTEGER
);
CREATE TABLE IF NOT EXISTS L0_t6 (
  c1 INTEGER,
  c2 INTEGER,
  c3 INTEGER,
  c4 INTEGER
);
CREATE TABLE IF NOT EXISTS L0_t7 (
  c1 INTEGER,
  c2 INTEGER,
  c3 INTEGER,
  c4 INTEGER,
  c5 INTEGER
);
CREATE TABLE IF NOT EXISTS L0_t8 (
  c1 INTEGER,
  c2 INTEGER,
  c3 INTEGER
);
CREATE TABLE IF NOT EXISTS L0_t9 (
  c1 INTEGER,
  c2 INTEGER,
  c3 INTEGER
);
CREATE TABLE IF NOT EXISTS L0_t10 (
  c1 INTEGER,
  c2 INTEGER,
  c3 INTEGER,
  c4 INTEGER
);
CREATE TABLE IF NOT EXISTS L1_t1 (
  c1 INTEGER,
  c2 INTEGER,
  c3 INTEGER
);
CREATE TABLE IF NOT EXISTS L1_t2 (
  c1 INTEGER,
  c2 INTEGER,
  c3 INTEGER,
  c4 INTEGER,
  c5 INTEGER
);
CREATE TABLE IF NOT EXISTS L1_t3 (
  c1 INTEGER,
  c2 INTEGER,
  c3 INTEGER,
  c4 INTEGER
);
CREATE TABLE IF NOT EXISTS L2_t1 (
  c1 INTEGER,
  c2 INTEGER,
  c3 INTEGER
);
CREATE TABLE IF NOT EXISTS L2_t2 (
  c1 INTEGER,
  c2 INTEGER,
  c3 INTEGER
);
CREATE TABLE IF NOT EXISTS L2_t3 (
  c1 INTEGER,
  c2 INTEGER,
  c3 INTEGER
);
CREATE TABLE IF NOT EXISTS L3_t1 (
  c1 INTEGER,
  c2 INTEGER,
  c3 INTEGER
);
CREATE TABLE IF NOT EXISTS L3_t2 (
  c1 INTEGER,
  c2 INTEGER,
  c3 INTEGER
);
CREATE TABLE IF NOT EXISTS L3_t3 (
  c1 INTEGER,
  c2 INTEGER,
  c3 INTEGER
);
CREATE TABLE IF NOT EXISTS L3_t4 (
  c1 INTEGER,
  c2 INTEGER,
  c3 INTEGER
);
CREATE TABLE IF NOT EXISTS L3_t5 (
  c1 INTEGER,
  c2 INTEGER,
  c3 INTEGER
); 
''',
    dag=dag
)

t1 = PostgresOperator(
    task_id='L0_task_1',
    postgres_conn_id='postgres_default',
    sql='''
INSERT INTO L1_t1 (c1, c2, c3)
SELECT t1.c1, t2.c2, t3.c3
FROM L0_t1 AS t1
INNER JOIN L0_t2 AS t2 ON t1.c1 = t2.c1
INNER JOIN L0_t3 AS t3 ON t2.c1 = t3.c1;
''',
    dag=dag
)

t2 = PostgresOperator(
    task_id='L0_task_2',
    postgres_conn_id='postgres_default',
    sql='''
INSERT INTO L1_t2 (c1, c2, c3, c4, c5)
SELECT t3.c1, t4.c2, t5.c3, t6.c4, t7.c5
FROM L0_t3 AS t3
INNER JOIN L0_t4 AS t4 ON t3.c1 = t4.c1
INNER JOIN L0_t5 AS t5 ON t4.c1 = t5.c1
INNER JOIN L0_t6 AS t6 ON t5.c1 = t6.c1
INNER JOIN L0_t7 AS t7 ON t6.c1 = t7.c1;
''',
    dag=dag
)

t3 = PostgresOperator(
    task_id='L0_task_3',
    postgres_conn_id='postgres_default',
    sql='''
INSERT INTO L1_t3 (c1, c2, c3, c4)
SELECT t6.c1, t8.c2, t9.c3, t10.c4
FROM L0_t6 AS t6
INNER JOIN L0_t8 AS t8 ON t6.c1 = t8.c1
INNER JOIN L0_t9 AS t9 ON t8.c1 = t9.c1
INNER JOIN L0_t10 AS t10 ON t9.c1 = t10.c1;
''',
    dag=dag
)

t4 = PostgresOperator(
    task_id='L1_task_1',
    postgres_conn_id='postgres_default',
    sql='''
INSERT INTO L2_t1 (c1, c2)
SELECT t1.c1, t2.c2
FROM L1_t1 AS t1
INNER JOIN L1_t2 AS t2 ON t1.c3 = t2.c3;
''',
    dag=dag
)

t5 = PostgresOperator(
    task_id='L1_task_2',
    postgres_conn_id='postgres_default',
    sql='''
INSERT INTO L2_t2 (c1)
SELECT t1.c1
FROM L1_t1 AS t1;
;''',
    dag=dag
)


t6 = PostgresOperator(
    task_id='L1_task_3',
    postgres_conn_id='postgres_default',
    sql='''
INSERT INTO L2_t3 (c1, c2)
SELECT t2.c1, t3.c2
FROM L1_t2 AS t2
INNER JOIN L1_t3 AS t3 ON t2.c3 = t3.c3;
''',
    dag=dag
)

t7 = PostgresOperator(
    task_id='L2_task_1',
    postgres_conn_id='postgres_default',
    sql='''
INSERT INTO L3_t1 (c1, c2)
SELECT t1.c1, t2.c2
FROM L2_t1 AS t1
INNER JOIN L2_t2 AS t2 ON t2.c1 = t1.c1;
''',
    dag=dag
)

t8 = PostgresOperator(
    task_id='L2_task_2',
    postgres_conn_id='postgres_default',
    sql='''
INSERT INTO L3_t2 (c1)
SELECT t2.c1
FROM L2_t2 AS t2;
''',
    dag=dag
)

t9 = PostgresOperator(
    task_id='L2_task_3',
    postgres_conn_id='postgres_default',
    sql='''
INSERT INTO L3_t3 (c1, c2)
SELECT t2.c1, t3.c2
FROM L2_t2 AS t2
INNER JOIN L2_t3 AS t3 ON t3.c1 = t2.c1;
''',
    dag=dag
)

t10 = PostgresOperator(
    task_id='L2_task_4',
    postgres_conn_id='postgres_default',
    sql='''
INSERT INTO L3_t4 (c1)
SELECT t3.c1
FROM L2_t3 AS t3;
''',
    dag=dag
)

t11 = PostgresOperator(
    task_id='L2_task_5',
    postgres_conn_id='postgres_default',
    sql='''
 INSERT INTO L3_t5 (c1)
SELECT t3.c1
FROM L2_t3 AS t3;
''',
    dag=dag
)

populate = PostgresOperator(
    task_id='populate',
    postgres_conn_id='postgres_default',
    sql='''
    INSERT INTO L0_t1 (c1, c2, c3)
VALUES (1, 1, 1);
INSERT INTO L0_t2 (c1, c2, c3)
VALUES (1, 1, 1);
INSERT INTO L0_t3 (c1, c2, c3)
VALUES (1, 1, 1);
INSERT INTO L0_t4 (c1, c2, c3)
VALUES (1, 1, 1);
INSERT INTO L0_t5 (c1, c2, c3)
VALUES (1, 1, 1);
INSERT INTO L0_t6 (c1, c2, c3, c4)
VALUES (1, 1, 1, 1);
INSERT INTO L0_t7 (c1, c2, c3, c4, c5)
VALUES (1, 1, 1, 1, 1);
INSERT INTO L0_t8 (c1, c2, c3)
VALUES (1, 1, 1);
INSERT INTO L0_t9 (c1, c2, c3)
VALUES (1, 1, 1);
INSERT INTO L0_t10 (c1, c2, c3, c4)
VALUES (1, 1, 1, 1);
INSERT INTO L1_t1 (c1, c2, c3)
VALUES (1, 1, 1);
INSERT INTO L1_t2 (c1, c2, c3)
VALUES (1, 1, 1);
INSERT INTO L1_t3 (c1, c2, c3)
VALUES (1, 1, 1);
INSERT INTO L2_t1 (c1, c2, c3)
VALUES (1, 1, 1);
INSERT INTO L2_t2 (c1, c2, c3)
VALUES (1, 1, 1);
INSERT INTO L2_t3 (c1, c2, c3)
VALUES (1, 1, 1);
INSERT INTO L3_t1 (c1, c2, c3)
VALUES (1, 1, 1);
INSERT INTO L3_t2 (c1, c2, c3)
VALUES (1, 1, 1);
INSERT INTO L3_t3 (c1, c2, c3)
VALUES (1, 1, 1);
INSERT INTO L3_t4 (c1, c2, c3)
VALUES (1, 1, 1);
INSERT INTO L3_t5 (c1, c2, c3)
VALUES (1, 1, 1);
''',
    dag=dag
)


t0 >> populate >> t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7 >> t8 >> t9 >> t10 >> t11