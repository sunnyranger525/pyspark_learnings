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

print ("============Load DATA ================")
spark = SparkSession.builder.appName("skew_data").getOrCreate()
data = [
    (100, 10),
    (100, 20),
    (100, 30),
    (200, 5),
    (999999, 50),
    (999999, 40),
    (999999, 55),
    (999999, 60),
    (999999, 65)
]

schema = ["id", "value"]

df = spark.createDataFrame(data, schema)
df.show()
df.printSchema()

# find key counts
id_key_count = df.groupBy("id").agg(count("*").alias("cnt"))
id_key_count.show()

# apply approxQuantile mechnisam to detect keys

skew_detect = (
    id_key_count.approxQuantile(
        "cnt", [0.90], 0.05
    )[0]
)
print(skew_detect)

keys_filter = id_key_count.filter(col("cnt") >= 3).select("id")
keys_filter.show()
keys_rdd  = keys_filter.rdd.flatMap(lambda x : x).collect()

df_salt_key = (
    df
    .withColumn(
    "salt_key",
    when(col("id").isin(keys_rdd), floor(rand() * 2).cast("int"))
    .otherwise(lit(2))
    )
    .withColumn(
        "join_saly_key",
        concat_ws("_", col("salt_key"), col("id"))
    )
)
df_salt_key.show()
df_salt_key.printSchema()

