import os
import urllib.request
import ssl
from typing import final

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

conf = SparkConf()\
    .setMaster("local[*]") \
    .set("spark.driver.host","localhost") \
    .set("spark.default.parallelism", "1")

sc = SparkContext(conf=conf)

#spark.read.format("csv").load("data/test.txt") \
    #.toDF("Success").show(20, False)

##################🔴🔴🔴🔴🔴🔴 -> DON'T TOUCH ABOVE CODE -- TYPE BELOW ####################################

spark = SparkSession.builder.appName("Word_count").getOrCreate()

words = [
    ("sam hari sam hima",),
    ("sam samsu hima sagar",),
    ("chandu chandu samsu sagar",)
]

cols = ["words"]

df = spark.createDataFrame(words, cols)

df.show()
df.cache()
df_split = (
    df.withColumn(
    "words_split",
    expr("explode(split(words, ' '))")
    )
    .groupBy("words_split")
    .agg(
        count("*").alias("word_cnt")
    )
)
df_split.show()

df_split = (
    df
    .withColumn(
        "split",
        split("words", ' ')
    )
    .withColumn(
        "rep",
        regexp_replace("words", " ", "@")
    )
    .withColumn(
        "slice_new",
        slice("split", 1, 2)
    )
    .withColumn(
        "words_sub",
        substring(col("words"), 1, 2)
    )
)
df_split.show()