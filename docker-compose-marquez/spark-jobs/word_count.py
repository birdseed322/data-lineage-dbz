from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("WordCount").getOrCreate()


# Load the input text file
inputFile = "file:///opt/bitnami/spark/data/word_count.txt"
inputRDD = spark.sparkContext.textFile(inputFile)

# Perform word count
wordCounts = inputRDD \
    .flatMap(lambda line: line.split(" ")) \
    .map(lambda word: (word, 1)) \
    .reduceByKey(lambda a, b: a + b)

# Print the word count results
for word, count in wordCounts.collect():
    print(f"{word}: {count}")

# Stop the SparkSession
spark.stop()
