from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from pyspark import SparkContext
# Create a SparkSession
spark = SparkSession.builder \
    .appName("transform") \
    .getOrCreate()

data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
#define columns
#columns = ["Names","Age"]
#create df
df = spark.createDataFrame(data,["Name", "Age"])

df = df.withColumn("Ageafter5years",col("Age")+5)
df.show()
df = df.withColumnRenamed("city", "loaction")
df.show()
df = df.drop("Ageafter5years")
df.show()
df = df.withColumn("Age", col("Age").cast("string"))
df.printSchema()