from pyspark.sql import SparkSession
from pyspark import SparkContext
# Create a SparkSession
spark = SparkSession.builder \
    .appName("MyFirstPySparkApp") \
    .getOrCreate()
sc = spark.sparkContext # SparkContext is required for RDD operations
ss = SparkSession.builder.getOrCreate()
