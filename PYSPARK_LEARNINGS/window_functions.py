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

print("==========Reading CSV File =============")
spark = SparkSession.builder.appName("CSV_File_Read").getOrCreate()

try:
    sale_df = spark.read.csv(
        r"E://DATA/csvfiles/input/sales.csv",
        header= True, inferSchema=True
    )
except Exception as e:
    print(f"path not found, please check")

sale_df.printSchema()
# Define Window functions
from pyspark.sql.window import *
part_spec = Window.partitionBy("country").orderBy(col("qty").desc())

# Assing dense rank based quantity saley
sale_dense_rank = (
    sale_df
    .withColumn(
        "rank_sale",
        dense_rank().over(part_spec)
    )
    .filter(col("rank_sale")==2)
    .groupby("country")
    .agg(
        count("*").alias("cnt"),
         min("qty").alias("qty_min"),
         max("qty").alias("qty_max"),
         sum("qty").alias("sum_qty"),
         avg("qty").alias("avg_qty")
    )
    .orderBy(col("sum_qty").desc())
)
sale_dense_rank.show()

# finding row level value and applying filter based on collect()
max_value = sale_dense_rank.collect()[3][4]
sale_filter = (
    sale_dense_rank
    .filter(col("sum_qty") == max_value)
)
sale_filter.show()
# show top 3 higest sales sum
win_spec = (
    Window
    .orderBy(col("sum_qty").desc())
)
win_spec2 = (
    Window
    .orderBy(col("sum_qty").desc())
    .rowsBetween(0,Window.unboundedFollowing)
)
top_3_sum_sale = (
    sale_dense_rank
    .withColumn(
        "row_num",
        row_number().over(win_spec)
    )
    .filter(col("row_num") >=3)
    .withColumn(
        "sum_run",
        sum("sum_qty").over(win_spec)
    )
    .withColumn(
        "avg_run",
        avg("sum_qty").over(win_spec).cast("double")
    )
    .withColumn(
        "percentage",
        ((col("sum_qty")/(sum("sum_qty").over(win_spec2)))*100).cast("double")
    )
)
top_3_sum_sale.show()
