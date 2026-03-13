import os
import urllib.request
import ssl
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys
python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path
os.environ['HADOOP_HOME'] =r"C:\hadoop"
#os.environ["PATH"] += ";C:\\hadoop\\bin"
os.environ['JAVA_HOME'] = r'C:\Program Files\Eclipse Adoptium\jdk-17.0.16.8-hotspot'
######################🔴🔴🔴################################
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 pyspark-shell'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-avro_2.12:3.5.4 pyspark-shell'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4 pyspark-shell'

conf = SparkConf().setAppName("pyspark") \
    .setMaster("local[*]") \
    .set("spark.driver.host","localhost") \
    .set("spark.default.parallelism", "1")

sc = SparkContext(conf=conf)

spark = SparkSession.builder.getOrCreate()

#spark.read.format("csv").load("data/test.txt") \
    #.toDF("Success").show(20, False)

##################🔴🔴🔴🔴🔴🔴 -> DON'T TOUCH ABOVE CODE -- TYPE BELOW ####################################

print("Loading Sales CSV file..............")
saledf = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(r"E:\DATA\csvfiles\input\sales02.csv")

print("Sale csv file has been loaded .........")

saledf.printSchema()
saledf.show()

print("loading product CSV file...........")
productdf = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(r"E:\DATA\csvfiles\input\product02.csv")

print("product csv file has been loaded ........")

productdf.printSchema()
productdf.show()

print("========Join 1 ======")

fulltable = (
    saledf.alias("s")
    .join(
        productdf.alias("p"),
        col("s.productid") == col("p.productid"),
        "inner"
    )
)
fulltable.show(5)

print("==================Join 2==================")
join02df = (
    saledf
    .join(
        productdf,
        "productid",
        "inner"
    )
)
join02df.show(5)

print("==============Join 3============")
s = saledf.alias("s1")
p = productdf.alias("p1")

join03df = (
    saledf
    .join(
        productdf,
        s.productid == p.productid,
        'inner'
    )
)
join03df.show(5)

seljoin3df =s.select (
    col("s1.orderid").alias("ordid"),
    col("s1.orderdate").alias("date")
)
seljoin3df.show()

print("============Join 4 =============")
