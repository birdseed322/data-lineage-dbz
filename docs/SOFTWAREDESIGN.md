This is the software design md file

#Limitations
SparkSubmitOperator - Need to explicitly specify name of python file in order for proper connections to be made between Airflow task and the Spark job that it calls

spark_default_conn - Manually set in Airflow UI. Host: spark://spark Port: 7077