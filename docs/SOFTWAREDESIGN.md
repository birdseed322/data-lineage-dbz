This is the software design md file

#Limitations
SparkSubmitOperator - Need to explicitly specify name of python file in order for proper connections to be made between Airflow task and the Spark job that it calls

spark_default_conn - Manually set in Airflow UI. Host: spark://spark Port: 7077
Requires AF / Spark jobs to be ran at least once to completion in order to retrieve full complete lineage
No upstream retrieving capabilities. Lineage of Airflow jobs are only constructed on downstream dags. Therefore, if a non-master dag is queried, there will not be a lineage connection between the master dag and the downstream dependent dags. However, only when a master dag is queried, then will the full complete data lineage be captured
taskId of airflow jobs must be unique even across DAGs
Spark Jobs should only write to one output (Have one lineage per spark job)
Databases / Sources need to be in lowercase, else it wont be captured
