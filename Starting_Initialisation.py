from pyspark.sql import SparkSession
from pyspark import SparkContext
# Create a SparkSession
spark = SparkSession.builder \
    .appName("MyFirstPySparkApp") \
    .getOrCreate()

# Print Spark version
print("Spark Version:", spark.version)
