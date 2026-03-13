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

# creating spark Session

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import boto3
from datetime import datetime, timedelta

# read from s3 on daily basis

process_date = (
    datetime.now() - timedelta(days = 1)
).strftime("%Y-%m-%d")
print(process_date)

source_path = f"s3a://samsu44/source/rawData/date={process_date}"
print(source_path)
target_path = f"s3a://samsu44/source/finalData/date={process_date}"
print(target_path)

# created data frame
try:
    raw_df = (
        spark
        .read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(source_path)
    )
except Exception as e:
    print("Error is ", e)
else:
    print("data is loaded sucessfull.....")
finally:
    print("Operation completed ..........")

# apply transformation

region_sale = Window.partitionBy(col("region")) \
    .orderBy(col("orderdate").desc()) \
    .rowsBetween(
    Window.unboundedPreceding,
    0
)
avg_sale = Window.partitionBy(col("region")) \
    .orderBy(col("orderdate").desc()) \
    .rowsBetween(
    Window.unboundedPreceding,
    Window.unboundedFollowing
)
try:
    run_sum_df = (
        raw_df
        .withColumn(
            "running_sum",
            sum("sale_amount").over(region_sale)
        )
        .withColumn(
            "avg_sale",
            avg(col("sale_amount")).over(avg_sale)
        )
    )
    run_sum_df.write.format("parquet").mode("overwrite") \
        .save(target_path)
except Exception as e1:
    print("e1")
else:
    print("No issues, done great")
finally:
    print("operation done")

import os
# if (sys.argv) > 1:
#     process = (sys.argv)[1]
# else:
#     process = (
#         datetime.now() - timedelta( days =1)
#     ).strftime("%Y-%m-%d")
