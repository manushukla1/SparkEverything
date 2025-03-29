from pyspark.sql import SparkSession
from pyspark import SparkContext
# Create a SparkSession
spark = SparkSession.builder \
    .appName("MyFirstPySparkApp") \
    .getOrCreate()
sc = spark.sparkContext # SparkContext is required for RDD operations
ss = SparkSession.builder.getOrCreate()
# convert list into rdd

lis = [1,2,3,4]
#parallelize is a method for converting list to rdd
rddin = sc.parallelize(lis , numSlices= 3) # Splitting into 3 partitions
print(rddin.collect())
print(rddin.getNumPartitions())
#if rdd is involved use Lambda

addrdd = rddin.map( lambda x : x + 2)
print(addrdd.collect())

filterrd = rddin.filter(lambda x :x>2) #filter method: move out the things which are not required
print(filterrd.collect())

liststr = ["zeyobron" , "zeyo" , "byte"]
rddstr = sc.parallelize(liststr)
replace = rddstr.filter(lambda x : "zeyo" in x.lower())
exactvalue = rddstr.filter (lambda  x : x.lower() == "zeyo")
print(exactvalue.collect())
print(replace.collect())

conrdd = rddstr.map(lambda x : x + "analytics")
print(conrdd.collect())
# map applies the function to every element of the list
replace = rddstr.map(lambda x : x.replace("zeyo","manu"))
print(replace.collect())

tild = ["A ~ B" , "C ~D " , "E ~ F"]
tildrdd = sc.parallelize(tild)
#   flatten each element with  ~ split
splitrdd = tildrdd.map(lambda x : x.split("~"))
print(splitrdd.collect())
splitrddd = tildrdd.flatMap(lambda x : x.split("~"))
print(splitrddd.collect())




