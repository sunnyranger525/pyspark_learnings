import os
import urllib.request
import ssl
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
spark = SparkSession.builder.appName("String_reverse").getOrCreate()
data = [
    ("apple",),
    ("banana",),
    ("cherry",),
    ("orange",),
    ("grape",)
]
from pyspark.sql.types import *
my_schema = StructType([
    StructField("name", StringType(), True)
])
raw_df = spark.createDataFrame(data, my_schema)

raw_df.show()
raw_df.printSchema()

add_col = (
    raw_df
    .withColumn(
        "split_name",
        split(col("name"), "")
    )
)
add_col.show()
reverse_string_df = (
    add_col
    .withColumn(
        "reverser_string",
        reverse(col("split_name"))
    )
    .withColumn(
        "sort",
        array_sort(col("reverser_string"))
    )
    .withColumn(
        "join",

        concat_ws("", col("reverser_string"))
    )
    .withColumn(
        "explode_split",
        explode(col("split_name"))
    )
)
reverse_string_df.show()

collect_data = (
    reverse_string_df
    .groupby("explode_split")
    .agg(
        count("*").alias("cnt"),
        collect_list(col("explode_split")).alias("list"),
        collect_set(col("explode_split")).alias("sets")
         )
)
collect_data.show()


