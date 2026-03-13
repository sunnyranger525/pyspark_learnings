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
spark = SparkSession.builder.appName("Union_method").getOrCreate()

list_of_list_01 = [
    ["Alice",  "F", 70000],
    ["Bob",    "M", 80000],
    ["Charlie","M", 55000],
    ["David",  "M", 45000],
    ["Eve",    "F", 50000]
]

data_01 = [ tuple(r) for r in list_of_list_01]

from pyspark.sql.types import *
schema_01 = StructType([
    StructField("name", StringType(), True),
    StructField("Gender", StringType(), True),
    StructField("Salary", IntegerType(), True )
])

df1 = spark.createDataFrame(data_01, schema_01)
def cols_case_correction(df):
    cols = df.columns
    for col in cols:
        new_col = col.lower()
        df = df.withColumnRenamed(col, new_col)
    return df
df2 = cols_case_correction(df1)
df2.show()


# define second dataframe

data_02 = [
    ("Frank",  "M", 60000),
    ("Grace",  "F", 65000),
    ("Hannah", "F", 70000),
    ("Ian",    "M", 48000),
    ("Jill",   "F", 53000)
]

from pyspark.sql.types import *
schema_01 = StructType([
    StructField("name", StringType(), True),
    StructField("Gender", StringType(), True),
    StructField("Salary", IntegerType(), True )
])

df3 = spark.createDataFrame(data_01, schema_01)

df4 = cols_case_correction(df3)
df4.show()

# union operations

df2_union_df4 = (
    df2.union(df1)
)
df2_union_df4.show()

df5 = (
    df4
    .withColumnRenamed("gender", "F/M")
)
df5.show()

# union by name

union_df2_df5 = (
    df2.unionByName(df5, allowMissingColumns=True)
)
union_df2_df5.show()