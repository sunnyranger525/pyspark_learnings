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

print("==========Reading URL Json file =============")

import requests
import json
# url path
url_path = r"https://randomuser.me/api/0.8/?results=5"

# create response
res_url_data = requests.get(url_path)

# check connection seccessed or not
print(res_url_data.status_code)

# check the data from url
#print(res_url_data.text)

# convert into dict
res_dict = res_url_data.json()
print(type(res_dict))

rdd1 = sc.parallelize([res_dict])

df = spark.read.json(rdd1)
df.show()

# convert array into struct type using explode method
df1 = df.withColumn(
    "results",
    explode(col("results"))
)
df1.printSchema()

# flatten the data using select or selectExpr
df2 = df1.selectExpr(
    "nationality",
    "results.user.cell",
    "results.user.dob",
    "results.user.email",
    "results.user.location.city",
    "results.user.location.state",
    "results.user.location.zip",
    "results.user.name.first",
    "results.user.name.last",
    "results.user.password",
    "results.user.phone",
    "seed",
    "version"
)
df2.printSchema()










