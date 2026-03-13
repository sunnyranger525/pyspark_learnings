import os
import urllib.request
import ssl
from bdb import effective
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

print("program for SCD Tables")

spark = (
    SparkSession.builder.appName("SCD_type2_table")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

# defin path first for delta tables
BASE_PATH = "file:///E:/deltatable/output/delta"
TABLE_PATH = BASE_PATH + "/customers"

# define schema
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType, BooleanType
from pyspark.sql.functions import current_timestamp, lit

schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("address", StringType(), True),
    StructField("effective_from", TimestampType(), False),
    StructField("end_date", TimestampType(), True),
    StructField("is_current", BooleanType(), False),
])
# create empty table based on schema
empty_df = spark.createDataFrame([], schema)
empty_df.write.format("delta").mode("overwrite").save(TABLE_PATH)

from delta.tables import DeltaTable

target_path = r"E://DATA/deltatable/output/delta/customer"

target_tab = DeltaTable.forPath(spark, target_path)

(
    target_tab.alias("t")
    .merge(
        empty_df.alias("s"),
        "t.customer_id = s.customer_id AND is_current = 1"
    )
    .whenMatchedUpdate(
        condition=
        "t.name <> s.name OR"
        "t.email <> s.email OR"
        "t.address <> s.address"
        "OR t.name is null"
        "OR t.address is null OR"
        "s.name is null OR s.address is null",
        set = {
            "is_current" : lit(0),
            "end_date" : current_timestamp()
        }
    )
    .whenNotMatchedInsert(
        values = {
            "customer_id" : "s.customer_id",
            "name" : "s.name",
            "address" : "s.address",
            "email" : "s.email",
            "effective_from" : current_timestamp(),
            "end_date" : lit(None),
            "is_current" : lit(1)
        }
    )
    .execute()
)

# spark Sql method
spark.sql("""
merget into target_tab as t 
using empty_df as s
on t.customer_id = s.customer_id AND is_current = 1
whne matched AND (
    t.name <> s.name OR t.email <> s.email
    OR t.address <> s.address Or t.name is null
    OR s.name is null OR t.address is null Or 
    s.address is null
)
then update set (
    t.end_date = current_timestamp().
    t.is_current = 0
)
when not matched 
then insert (
    customer_id, name, email, address, effective_from, end_date, is_current
) values (
    s.customer_id,
    s.name,
    s.email,
    s.address,
    current_timestamp(),
    None,
    1
)
""")


