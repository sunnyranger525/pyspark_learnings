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

print("==========Reading URL Json file =============")

# import os (its an optional)

import os

# import url request library its statndard

import urllib.request

# import ssl (secure socket layer) secure connections
import ssl

urldata = urllib.request \
    .urlopen(
    "https://randomuser.me/api/0.8/?results=5",
    context= ssl._create_unverified_context()
).read().decode("utf-8")

# Display the url data
print(urldata)
print("URL data loaded successful.........")

# convert load data into rdd

rdd1 = sc.parallelize([urldata])

# convert into data frame

urldf = spark.read.json(rdd1)
urldf.show()
urldf.printSchema()

# explode urldf where havign Arry schema

explodedf = (
    urldf
    .withColumn(
        "results", expr("explode(results)")
    )
)
explodedf.printSchema()

# flatten json data using select or with column

selflatten =(
    explodedf
    .select(
        "nationality",
        "results.user.cell",
        "results.user.dob",
        "results.user.email",
        "results.user.location.state",
        "results.user.md5",
        "results.user.name.title",
        "seed",
        "version"
    )
)
#selflatten.show()
selflatten.printSchema()



