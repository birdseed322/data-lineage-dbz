from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

##########################
# You can configure master here if you do not pass the spark.master paramenter in conf
##########################
master = "spark://spark:7077"
conf = SparkConf().setAppName("Spark Hello World").setMaster(master)
spark = SparkSession.builder.config(conf=conf).getOrCreate()

# Print result
print("This is a spark job")

spark.stop()