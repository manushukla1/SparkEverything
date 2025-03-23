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

df.select("Name", "Age").show()
df.filter(df.Age>25).show()
df.groupBy("city")
df.orderBy("Age", ascending = True).show()