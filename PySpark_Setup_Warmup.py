from tkinter.messagebox import showerror

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Day1").getOrCreate()
df = (spark.read.
      csv("/Users/manushukla/Downloads/netflix_titles.csv", header=True, inferSchema=True))
df.printSchema()
df.show()
df.select("title").show()
df.selectExpr("len(title)>10").show()
df.filter((df["rating"].isNull())&(df["director"].isNull())).show()
df_clean = df.na.drop(subset=["title"]).show()
df_filled = df.na.fill({"rating":"MS"})
df_filled.show(100) # never use .show with in the data frame as it will not return the dataframe only displays it
df_set = df_filled.filter("rating = 'MS'")
df_set.show()
df.filter("title = 'Love'").show(truncate=False)
df.filter("release_year > '2015'").show()
df.selectExpr("length(title) as title_length").show()
df.groupBy("type").count().show()
df.groupBy("release_year").count().orderBy("release_year").show()