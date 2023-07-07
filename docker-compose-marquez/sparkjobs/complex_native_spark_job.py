from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

# Create a SparkSession
spark = SparkSession.builder.appName("complex_native_spark_job").getOrCreate()

# JDBC connection properties
jdbc_url = "jdbc:postgresql://postgres:5432/airflow"
connection_properties = {
    "user": "postgres",
    "password": "",
    "driver": "org.postgresql.Driver",
}

# Read data from PostgreSQL table
visits_df = spark.read.jdbc(
    url=jdbc_url, table="visits", properties=connection_properties
)
page_views_df = spark.read.jdbc(
    url=jdbc_url, table="page_views", properties=connection_properties
)
page_categories_df = spark.read.jdbc(
    url=jdbc_url, table="page_categories", properties=connection_properties
)

result1 = (
    visits_df.withColumn("visit_date", col("visit_date").cast("date"))
    .filter((col("visit_date") >= "2023-01-01") & (col("visit_date") <= "2023-12-31"))
    .groupBy("user_id")
    .count()
    .withColumnRenamed("count", "num_visits")
    .limit(10)
)

# result1 = spark.sql(
#     """
#  	SELECT user_id, COUNT(*) AS num_visits
#     FROM visits
#     WHERE visit_date BETWEEN '2023-01-01' AND '2023-12-31'
#     GROUP BY user_id
#     LIMIT 10;
# """
# )

result1.show()

joined_df = page_views_df.join(page_categories_df, "page_id")

result2 = (
    joined_df.filter(col("category") == "Technology")
    .groupBy("page_id")
    .agg(avg("duration").alias("avg_duration"))
    .orderBy(col("avg_duration").desc())
    .limit(10)
)

# result2 = spark.sql(
#     """
#         SELECT page_views.page_id, AVG(duration) AS avg_duration
#         FROM page_views
#         JOIN page_categories ON page_views.page_id = page_categories.page_id
#         WHERE page_categories.category = 'Technology'
#         GROUP BY page_views.page_id
#         ORDER BY avg_duration DESC
#         LIMIT 10
# """
# )

result2.show()

# Write results to parquet files
result1.write.mode("overwrite").parquet(
    "file:///usr/local/spark/app/output/result1_native_spark.parquet"
)
result2.write.mode("overwrite").parquet(
    "file:///usr/local/spark/app/output/result2_native_spark.parquet"
)

# Stop the SparkSession
spark.stop()

# To run this spark job manually, copy the following command into the Spark container's terminal

"""
spark-submit --conf "spark.extraListeners=io.openlineage.spark.agent.OpenLineageSparkListener" \
    --packages "io.openlineage:openlineage-spark:0.27.2" \
    --conf "spark.openlineage.transport.url=http://marquez:5000/api/v1/namespaces/example/" \
    --conf "spark.openlineage.transport.type=http" \
    --jars /usr/local/spark/app/postgresql-42.6.0.jar complex_native_spark_job.py
"""
