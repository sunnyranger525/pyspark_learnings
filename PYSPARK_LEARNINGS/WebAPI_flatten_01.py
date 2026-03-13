import os
import urllib.request
import ssl
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys

from urllib3 import request

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

print("==========Reading URL Json file format =============")

import requests
import json

# web url
web_url = "https://randomuser.me/api/0.8/?results=5"

# define variable to store response data into variable

res_data = requests.get(web_url)
print("=======Checking type of data responded........")
print(type(res_data))
print("=======checking repsonse status code")
print(res_data.status_code)

# Converting nested json to python dict
res_data_dict = res_data.json()
print(type(res_data_dict))

from pyspark.sql import functions as f
rdd1 = sc.parallelize([res_data_dict])

web_df = (
    spark.read.json(rdd1)
)
#flatten it array data which is having
web_df1 = (
    web_df
    .withColumn(
        "results",
        f.explode(f.col("results"))
    )
)

web_df2 = (
    web_df1
    .select(
        f.col("nationality"),
        f.col("results.user.cell"),
        f.col("results.user.email"),
        f.col("results.user.gender"),
        f.col("results.user.location.city"),
        f.col("results.user.location.state"),
        f.col("results.user.location.zip"),
        f.col("results.user.name.first"),
        f.col("seed"),
        f.col("version")
    )
)
web_df.printSchema()
web_df1.printSchema()
web_df2.show()







