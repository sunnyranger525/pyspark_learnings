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
#saledf.show()

print("loading product CSV file...........")
productdf = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(r"E:\DATA\csvfiles\input\product02.csv")

print("product csv file has been loaded ........")

productdf.printSchema()
#productdf.show()
print("==================Join==================")
fulldata = (
    saledf
    .join(
        productdf,
        "productid",
        "inner"
    )
    .select(
        col("productid").alias("prodid"),
        col("orderid").alias("ordid"),
        col("orderdate").alias("date"),
        col("product_name").alias("prodname")
    )
)
fulldata.show(5)

print("=========orders from last one month =======")
saledf_last_1_month = (
    fulldata
    .filter(
        col("date") >= date_sub(current_date(), 30)
    )
    .orderBy(col("date").asc())
)
saledf_last_1_month.show()

print("===========Orders from last 7 week==== ")
orders_last_7_days = (
    fulldata
    .filter(
        col("date") >= date_sub(current_date(), 7)
    )
    .orderBy(col("date").desc())
)
orders_last_7_days.show()

print("=========orders from previous 1st =======")

orders_previous_month = (
    fulldata
    .filter(
        datediff(current_date(),col("date")) <= 30
    )
    .orderBy(col("date").asc())
)
orders_previous_month.show()

print("===========orders from previous month 1st=")

orders_pre_mont_1st = (
    fulldata
    .filter(
        (col("date") >= trunc(date_sub(current_date(), 30), "month"))
        &
        (col("date") <= last_day(date_sub(current_date(), 30)))
    )
    .orderBy(col("date").desc())
)
orders_pre_mont_1st.show()
