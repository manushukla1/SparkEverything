from pyspark.sql import SparkSession
# Create a SparkSession
spark = SparkSession.builder \
    .appName("Myreadfile") \
    .getOrCreate()

df = spark.read.format("csv").option("header","true").load("/Users/manushukla/Downloads/reading.csv")
df.show()  # This should work
df.write.mode("overwrite").csv("/Users/manushukla/Downloads/output.csv") # have taken the data from above file and created a new folder where i have dumped it
