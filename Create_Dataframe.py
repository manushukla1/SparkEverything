from pyspark.sql import SparkSession
from pyspark import SparkContext
# Create a SparkSession
spark = SparkSession.builder \
    .appName("MyFirstPySparkApp") \
    .getOrCreate()
sc = SparkContext
ss = SparkSession.builder.getOrCreate()
#below is the data
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
#define columns
#columns = ["Names","Age"]
#create df
df = spark.createDataFrame(data,["Name", "Age"])
df.show()
df.printSchema()
print("Total Rows:", df.count())
df.select("Name").show()
df.filter(df.Age > 25).show()
df.groupBy("Age").count().show()