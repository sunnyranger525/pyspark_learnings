import os
import urllib.request
import ssl
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

print("==========Reading Local DATA =============")
spark = SparkSession.builder.appName("Array").getOrCreate()
data = [
    ("liSten", "silent"),
    ("spark", "Sarks"),
    ("hEllo", "world"),
    ("Triangle", "Integral")
]

# ddl method to define schema

schema_cols = ["str1", "str2"]

# create data frame

df = spark.createDataFrame(data, schema_cols)
df.show()
df.cache()

df1 = (
    df
    .withColumn(
        "array1",
        split(lower(col("str1")),"")
    )
    .withColumn(
        "array2",
        split(lower(col("str2")), "")
    )
    .withColumn(
        "array_",
        array_max(col("array1"))
    )
)
df1.show()

# var= input("emter some valu2 :>>: ")