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
# second data frame creation
data2 = [(1, "NY"),
         (2, "SF"),
         (4, "LA")]

column2 = ["ID" , "City"]
df2 =  spark.createDataFrame(data2,column2)
df2.show()

# Inner join--- 📌 Use When:
#You only need the matching records from both DataFrames.Example: Finding employees who have a city assigned to them.
"""🛑 What to Remember?
Rows that don’t match in both DataFrames are removed.If an ID is in one DataFrame but not in the other, it won’t appear in the result."""

df1.join(df2, on="ID", how="inner").show()                       #----inner join

# left Join--- 📌 Use When:
# You need all records from the left DataFrame and matching ones from the right.Example: You want all employees, even if they don’t have a city assigned.
"""🛑 What to Remember?
All records from df1 remain, even if there’s no match in df2.Missing values from df2 will appear as NULL."""

df1.join(df2, on="ID", how="left").show()                       #----Left join

# Rigth join--- 📌 Use When:
# You need all records from the right DataFrame and matching ones from the left.Example: You want all cities, even if they don’t have an employee assigned.
"""🛑 What to Remember?
All records from df2 remain, even if there’s no match in df1. Missing values from df1 will appear as NULL."""

df1.join(df2, on="ID", how="right").show()                       #----RightJoin

# full outer join--- Use When:
# You need all records from both DataFrames, even if they don’t match.Example: Getting a full list of employees and cities, even if some don’t have a match.
"""🛑 What to Remember?
All records are included.Missing values in either DataFrame will appear as NULL."""

df1.join(df2, on="ID", how="outer").show()                        #----Full-Outer Join

#Cross Join--- 📌 Use When:
#You want every row from df1 to combine with every row from df2. Example: Generating all possible combinations of employees and cities.
"""What to Remember?Can explode in size if DataFrames are large. Be careful! 🚨No common column is required."""

df1.crossJoin(df2).show()                                       #----Cross-Join 