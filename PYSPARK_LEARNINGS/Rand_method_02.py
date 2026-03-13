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
sale_df.show(5)

# finding max key count based on country

key_count_df = sale_df.groupBy("country").agg(
    count("*").alias("cnt_country")
)
key_count_df.show()
max_key = key_count_df.agg(max("cnt_country").alias("max")).collect()[0][0]
print(max_key)
min_key = key_count_df.agg(min("cnt_country").alias("min")).collect()[0][0]
print(min_key)
# define function to add salt key if find data skew
def call_saltkey(max_key, min_key, sale_df):
    if max_key > (min_key * 3):
        return sale_df.withColumn("salt_key", floor(rand() *  10))
    else:
        return sale_df.withColumn("salt_key", lit(0))
# calling data skew test function and if skew add salt key
sale_df_skew = call_saltkey(max_key, min_key, sale_df)
sale_df_skew.show(10)
