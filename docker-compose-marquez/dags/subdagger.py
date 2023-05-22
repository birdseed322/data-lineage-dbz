from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.utils.dates import datetime, days_ago

default_args = {
    'owner': 'eze',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'email': ['datascience@example.com']
}

# Define a SubDAG
def create_subdag(parent_dag_name, child_dag_name, args):
    subdag = DAG(
        dag_id=f'{parent_dag_name}.{child_dag_name}',
        default_args=args,
        schedule_interval="@daily"
    )

    def print_message():
        print(f"This is a task in {subdag.dag_id}")

    with subdag:
        task_1 = PythonOperator(task_id='task_1', python_callable=print_message)
        task_2 = PythonOperator(task_id='task_2', python_callable=print_message)

        task_1 >> task_2

    return subdag

# Define the main DAG
dag = DAG(
    'subdag_test',
    schedule_interval='*/1 * * * *',
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    default_args=default_args,
    description='DAG for subdag testing.'
)

# Define the tasks in the main DAG
start = PythonOperator(task_id='start', python_callable=lambda: print("Starting the DAG"), dag=dag)

# Create SubDAGs and assign them as tasks in the main DAG
subdag_1 = SubDagOperator(
    task_id='subdag_1',
    subdag=create_subdag('subdag_test', 'subdag_1', dag.default_args),
    dag=dag,
)

subdag_2 = SubDagOperator(
    task_id='subdag_2',
    subdag=create_subdag('subdag_test', 'subdag_2', dag.default_args),
    dag=dag,
)

end = PythonOperator(task_id='end', python_callable=lambda: print("DAG completed"), dag=dag)

# Define the task dependencies
start >> subdag_1 >> subdag_2 >> end
