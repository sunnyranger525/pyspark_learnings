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

print("==========Loading csv Sale data file =============")

saledf = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(r"E:/DATA/csvfiles/input/sales.csv")
saledf.printSchema()
#saledf.show(5)

print("======load csv file product data =========")
productdf = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(r"E:/DATA/csvfiles/input/products.csv")
productdf.printSchema()
#productdf.show(5)

print("====sale data total price based on country ===")

print("===========join the both tables ======")

rawdf  = (
    saledf
    .join(
        productdf,
        saledf["productid"] == productdf["id"],
        "inner"
    )
)
rawdf.show(5)

print("====filter required columns ====")
from pyspark.sql.types import *
colfilter = (
    rawdf
    .selectExpr(
        "country",
        "category",
        "qty",
        "price",
        "qty * price as Amount"
    )
    .withColumn("Amount", round(col("Amount"), 2))
)
colfilter.show(truncate= False)
colfilter.printSchema()
categorygfdf = (
    colfilter
    .groupby("country", "category")
    .agg(
        sum(col("Amount")).alias("Amt"),
        sum(col("qty")).alias("qty"),
        sum(col("price")).alias("price")
    )
    .withColumn("Amt", round(col("Amt"), 2))
    .withColumn("price", round(col("price"), 2))
)
categorygfdf.show(10)

print("==============Filer country wise ==")

from pyspark.sql.window import *

windowpart = (
    Window
    .partitionBy("country").orderBy(col("Amt").desc())
    )
Country_rank = (
    categorygfdf
    .withColumn("Rank", rank().over(windowpart))
    .filter("Rank <= 3")
    .orderBy(col("price").desc())
)
Country_rank.show(10)