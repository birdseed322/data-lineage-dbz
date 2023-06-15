from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("complex_spark_job").getOrCreate()

# JDBC connection properties
jdbc_url = "jdbc:postgresql://postgres:5432/airflow"
connection_properties = {
    "user": "postgres",
    "password": "",
    "driver": "org.postgresql.Driver"
}

# Read data from PostgreSQL table
spark.read.jdbc(url=jdbc_url, table="visits", properties=connection_properties).createOrReplaceTempView("visits")
spark.read.jdbc(url=jdbc_url, table="page_views", properties=connection_properties).createOrReplaceTempView("page_views")
spark.read.jdbc(url=jdbc_url, table="page_categories", properties=connection_properties).createOrReplaceTempView("page_categories")


# Perform complex SQL queries
result1 = spark.sql("""
 	SELECT user_id, COUNT(*) AS num_visits
    FROM visits
    WHERE visit_date BETWEEN '2023-01-01' AND '2023-12-31'
    GROUP BY user_id
    LIMIT 10;
""")

result2 = spark.sql("""
        SELECT page_views.page_id, AVG(duration) AS avg_duration
        FROM page_views
        JOIN page_categories ON page_views.page_id = page_categories.page_id
        WHERE page_categories.category = 'Technology'
        GROUP BY page_views.page_id
        ORDER BY avg_duration DESC
        LIMIT 10
""")

result2.show()

# Write results to parquet files
result1.write.mode("overwrite").parquet("file:///usr/local/spark/app/output/result1.parquet")
result2.write.mode("overwrite").parquet("file:///usr/local/spark/app/output/result2.parquet")

# Stop the SparkSession
spark.stop()
