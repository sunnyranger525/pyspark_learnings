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

print("==========Reading Local DATA =============")
spark = SparkSession.builder.appName("vowels").getOrCreate()
data = [
    (101, "I love to play cricket"),
    (102, "I am into motorbiking"),
    (104, "What do you like")
]
schema_data = ["id", "str1"]

df_string = spark.createDataFrame(data, schema_data)
df_string.show()

# need only, consonates letters and join all

consonates_df = (
    df_string
    .withColumn("lower_str1", lower(col("str1")))
    .withColumn(
        "consonates",
        regexp_replace(col("lower_str1"), "a|e|i|o|u|\s", "")

    )
    .withColumn("length_of_str", length(col("consonates")))
)
consonates_df.show()

# method two

str1_rep = ["a", "e", "i", "o", "u", " "]
cons_df = (
    df_string
    .withColumn(
        "lower_str1",
        lower(col("str1"))
    )
    .withColumn(
        "split",
        split(col("lower_str1"), "")
    )
    .withColumn(
        "filter",
        filter(col("split"), lambda x: x.isin(str1_rep) == False)
    )
    .withColumn(
        "join",
        array_join(col("filter"), "")
    )
)
cons_df.show()