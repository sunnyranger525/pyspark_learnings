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
spark = SparkSession.builder.appName("Anagram").getOrCreate()
data = [
    ("listen", "silent"),
    ("spark", "parks"),
    ("hello", "world"),
    ("Triangle", "Integral")
]

from pyspark.sql.types import *
my_schema = StructType([
    StructField("str1", StringType(), True),
    StructField("str2", StringType(), True)
])

raw_string_tab = spark.createDataFrame(data, my_schema)
raw_string_tab.printSchema()
raw_string_tab.show()

# extracting string

substr_df = (
    raw_string_tab
    .withColumn(
        "str1_ext",
        substring(col("str1"), 0, 2)
    )
    .withColumn(
        "length",
        length(col("str1"))-4
    )
    .withColumn(
        "str1_end_2",
        substring(col("str1"),length(col("str1"))-1, 2)
    )
    .withColumn(
        "mask",
        expr("repeat('*', length)")
    )
    .withColumn(
        "concat",
        concat(
            col("str1_ext"),
            col("mask"),
            col("str1_end_2")
        )
    )
)
substr_df.show()

# help(substr)
help(reversed)

def rev_str(str):
    rev_str = reversed(str)
    join_str = "".join(rev_str)
    return join_str
rev_string = udf(rev_str, StringType())

sub_str_df = (
    raw_string_tab
    .select(
        "*",
        expr("""
        concat(
        substr(str1, 1, 2),
        repeat('*', length(str1)-4),
        substr(str1, length(str1)-1, 2)
        ) as join
        """),
        expr("replace(join, '*', '#') as rep"),
        expr("length(rep) as len_mask"),
        reverse(col("str1")).alias("rev_str"),
        col("str1").rlike("^[0-9]+$").alias("str_3"),
        rev_string(col("str1"))
    )
)
sub_str_df.show()

