from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder.appName("SumJob").getOrCreate()

# Input data
numbers = [1, 2, 3, 4, 5]

# Create RDD from the list of numbers
rdd = spark.sparkContext.parallelize(numbers)

# Calculate the sum using RDD transformations and actions
total_sum = rdd.sum()

# Print the result
print("The sum of numbers is:", total_sum)

# Stop the SparkSession
spark.stop()