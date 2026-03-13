import os
from re import split
from struct import Struct

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path
os.environ['HADOOP_HOME'] =r"C:\hadoop"
os.environ['JAVA_HOME'] = r'C:\Program Files\Eclipse Adoptium\jdk-17.0.16.8-hotspot'
######################🔴🔴🔴################################
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 pyspark-shell'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-avro_2.12:3.5.4 pyspark-shell'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4 pyspark-shell'

conf = SparkConf() \
    .setMaster("local[*]") \
    .set("spark.driver.host","localhost") \
    .set("spark.default.parallelism", "1")

sc = SparkContext(conf=conf)

# spark = SparkSession.builder.getOrCreate()

#spark.read.format("csv").load("data/test.txt") \
    #.toDF("Success").show(20, False)

##################🔴🔴🔴🔴🔴🔴 -> DON'T TOUCH ABOVE CODE -- TYPE BELOW ####################################

print("==========RDD Opeations =============")
spark = SparkSession.builder.appName("RDD_operations").getOrCreate()
data = [3, 4, 5, 6,7,8,9,10]

rdd1 = sc.parallelize(data)

# it shows the all elements in the form of list
print(rdd1.collect())

# it shows each element on as one row
rdd1.foreach(print)

# applying map operations

map_rdd1 = rdd1.map(lambda x : x + 2)
print(map_rdd1.collect())

map_rdd2 = rdd1.map(lambda x : x*2)
print(map_rdd2.collect())

# reduce is like aggregate funcition sum
reduce_rdd = rdd1.reduce(lambda x, y : x+y)
print(reduce_rdd)

# filter functions
filter_rdd = rdd1.filter(lambda x : x%2 == 0)
print(filter_rdd.collect())

list01 = ["sam samsu", "hima sagar", "sagar", "hima"]
rdd2 = sc.parallelize(list01)
print(rdd2.collect())

# map to rdd2
map_split_rdd1 = rdd2.map(lambda x : x.split(" "))
print(map_split_rdd1.collect())

# flatMap to rdd2
flatmap_rdd1 = rdd2.flatMap(lambda x : x.split(" "))
print(flatmap_rdd1.collect())
map_valu2 =  flatmap_rdd1.map(lambda x : (x, 1))
print(map_valu2.collect())

# apply reduce by key
reduce_by_key_rdd = map_valu2.reduceByKey(lambda x, y : x+y)
print(reduce_by_key_rdd.collect())

# group by key
group_by_key = map_valu2.groupByKey().mapValues(list)
print(group_by_key.collect())
# group_by_key_1 = map_valu2.groupByKey().mapValues(lambda z : sum(z))
# print(group_by_key_1.collect())



