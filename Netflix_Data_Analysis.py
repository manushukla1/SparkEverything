from pyspark.sql import SparkSession  # session started
from pyspark.sql.functions import *
spark = SparkSession.builder.appName("Netflixprojectanalysis").getOrCreate()
#dataset load
df = spark.read.csv("/Users/manushukla/Downloads/netflix_titles.csv", header=True, inferSchema=True)
df.printSchema()
df.show(5)

#data cleaning

df_clean = df.dropna(subset=["title","type", "country"])
df_clean1 = df_clean.na.fill({"rating": "Unknown"})


#Add new column


df_add_column = df_clean1.withColumn("title_length", expr("len(title)"))
df_add_column1 = df_add_column.withColumn("is_movie", expr("Case When type == 'Movie' then 1 else 0 end "))
df_add_column1.show()
#analysis

df_add_column1.groupBy("country").count().orderBy("count", ascending = False).show(50)

df_add_column1.groupBy("release_year").count().orderBy("release_year").show(50)

df_add_column1.groupBy("type").count().show()

df_add_column1.groupBy("rating").count().orderBy("count", ascending=False).show()

df_add_column1.select("title", "title_length").orderBy(col("title_length").desc()).show(5)
#write back
df_clean.write.csv("/Users/manushukla/Downloads/clean.csv", header=True, mode="overwrite")

#done - mini project end