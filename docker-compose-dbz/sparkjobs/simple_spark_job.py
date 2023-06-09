from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder.appName("SimpleSparkJob").getOrCreate()

# Read locations to DataFrame and create a temporary view
locations = (
    spark.read.option("header", "true")
    .option("inferschema", "true")
    .csv("file:///usr/local/spark/app/input/locations.csv")
)
locations.createOrReplaceTempView("locations")

# Read user_favorites to DataFrame and create a temporary view
user_favorites = (
    spark.read.option("header", "true")
    .option("inferschema", "true")
    .csv("file:///usr/local/spark/app/input/user_favorites.csv")
)
user_favorites.createOrReplaceTempView("user_favorites")


# Join user_favorites and locations, and generate the nicknames
nicknames = spark.sql(
    """
SELECT
  user_favorites.id,
  CONCAT(
    favorite_color,
    ' ',
    state
  ) AS nickname
FROM user_favorites
JOIN locations
ON user_favorites.favorite_city = locations.city
"""
)

# Write output and print final DataFrame to console
nicknames.write.mode("overwrite").csv(
    "file:///usr/local/spark/app/output/nicknames.csv"
)
nicknames.show(20, False)
