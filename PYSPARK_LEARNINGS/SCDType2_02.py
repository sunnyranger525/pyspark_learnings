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

# define path
base_path = r"E:///DATA/deltatable/output/delta"
target_path = base_path + r"/sales"

schema = ["id", "name", "address", "effective_from", "end_date", "is_current"]
data = [
    "data"
]

# create empty dataframe
source_df = spark.createDataFrame(schema)

# write dataframe to target location
source_df.write.format("delta").mode("overwrite").save(target_path)

# import delta table
from delta.tables import DeltaTable

target_tab = DeltaTable.forPath(spark, target_path)

# perform merge statment in pyspark

target_tab.alias("t")\
    .merge(
    source_df.alias("s"),
    "t.id = s.id AND t.is_current = 1"
)\
    .whenMatchedUpdate(
    condition=
    "t.name <> s.name OR "
    "t.address <> s.address",
    set = {
        "end_date" : current_timestamp(),
        "is_current" : lit(0)
    }
)\
    .whenNotMatchedInsert(
    values={
        "id" : "s.id",
        "name": "s.name",
        "address" : "s.address",
        "effective_from": current_timestamp(),
        "end_date" : lit(None),
        "is_current": lit(1)
    }

).execute()

# define in spark sql


spark.sql("""
    merge into target_tab as t
    using source_df as s
    on t.id = s.id and t.is_current = 1
    when matched AND (
        t.name <> s.name OR
        t.address <> s.address  
    ) then update set 
        t.end_date = current_timestamp(),
        t.is_current = 0
    when not matched
        then insert (
            id,
            name,
            address,
            effective_from,
            end_date,
            is_current
        ) values (
            s.id,
            s.name,
            s.address,
            current_timestamp(),
            NULL,
            1
        )     
""")