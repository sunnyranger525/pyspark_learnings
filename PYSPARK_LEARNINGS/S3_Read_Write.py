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
"""
spark = SparkSession.builder.appName("read_S3").getOrCreate()
"""

process_date = (
    current_date - timedelta(days =1)
)

source_path = f"s3a://bucketname/sourcedata/date={process_date}/"
target_path = f"s3a://bucketname/target/date={process_date}/"

raw_df = spark.read.format("csv") \
    .option("header", 'true') \
    .option('inferSchema', 'true') \
    .option("path", source_path) \
    .load()

filter_df = raw_df.dropna().filter(col("amount") > 0) \
    .withColumn("date", date_sub(current_date(), 1))

filter_df.write.format("parquet") \
    .mode("overwrite") \
    .partitionBy("date") \
    .option("path", target_path) \
    .save()


jdbc_url = r"jdbc:postgres://red-endpoint/db"
table_name = "employee",
properties_read = {
    "username" : "samsu",
    "password" : "password",
    "driver" : "drive.com.postgres.end"
}

# reading data file from RDBMS

raw_df = (
    spark
    .read
    .jdbc(
        url = jdbc_url,
        table = table_name,
        properties= properties_read
    )
)

# Writing DAta to RDBMS
raw_df.write.jdbc(
    url = jdbc_url,
    table = table_name,
    mode = "append",
    properties= properties_read
)
raw_df.write.mode("append") \
    .parquet(r"s3a://bucket-name/tagert/data/")



