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

conf = SparkConf() \
    .setMaster("local[*]") \
    .set("spark.driver.host","localhost") \
    .set("spark.default.parallelism", "1")

sc = SparkContext(conf=conf)

# spark = SparkSession.builder.getOrCreate()

#spark.read.format("csv").load("data/test.txt") \
    #.toDF("Success").show(20, False)

##################🔴🔴🔴🔴🔴🔴 -> DON'T TOUCH ABOVE CODE -- TYPE BELOW ####################################

print("==========Reading URL Json file =============")

spark = SparkSession.builder.appName("URL_data_read").getOrCreate()

from urllib import request
import os
import ssl

# store url in variable
url_path = r"https://randomuser.me/api/0.8/?results=5"

# store ssl verification in var
context_verification = ssl.create_default_context()

# call url data using python libraries

url_data = (
    urllib.request
    .urlopen(
        url_path,
        context= context_verification
    )
    .read().decode("utf-8")
)
print(type(url_data))

# convert string url json into dict

import json

url_dict_data = json.loads(url_data)

print(type(url_dict_data))

# convert into rdd after parsed to dict

rdd_url_data = sc.parallelize([url_dict_data])


#convert into dataframe after parsed into rdd

url_df = spark.read.json(rdd_url_data)

url_df.show()

url_df.printSchema()

# flatterning arry with selectExpr

flattern_arry_expr = (
    url_df
    .selectExpr(
        "nationality",
        "explode(results)",
        "seed",
        "version"
    )
)
flattern_arry_expr.printSchema()

# flattening data

flatten_arry = (
    url_df
    .withColumn(
        "results",
        explode("results")
    )
)
flatten_arry.printSchema()

# converto to into inline data

flatten_struct = (
    flatten_arry
    .select(
        "nationality",
        "results.user.cell",
        "results.user.dob",
        "seed",
        "version"
    )
)
flatten_struct.printSchema()





