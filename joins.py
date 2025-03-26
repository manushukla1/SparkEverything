from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# initialize spark
spark = SparkSession.builder.appName("JoinsExample").getOrCreate()

# data frame creation
data1 = [(1, "Alice", "HR"),
         (2, "Bob", "IT"),
         (3, "Charlie", "Finance")]

column1 = ["ID" , "Name" , "Department"]

df1 =  spark.createDataFrame(data1,column1)
df1.show()

data2 = [(1, "NY"),
         (2, "SF"),
         (4, "LA")]

column2 = ["ID" , "City"]
df2 =  spark.createDataFrame(data2,column2)
df2.show()