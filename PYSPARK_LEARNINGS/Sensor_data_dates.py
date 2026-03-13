import os
import urllib.request
import ssl
from dataclasses import field

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys

from pyspark.sql.types import StructType

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

spark = SparkSession.builder.appName("Missing_dates_fill").getOrCreate()
from pyspark.sql.types import *
from pyspark.sql.functions import explode

sensor_data = [
    ("S1", "2025-11-01", 30.2),
    ("S1", "2025-11-02", 31.1),
    ("S1", "2025-11-04", 29.8),
    ("S2", "2025-11-01", 25.4),
    ("S2", "2025-11-04", 26.0)
]

my_cols = StructType(
    [
        StructField("id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("temp", StringType(), True)
    ]
)

df_sensor_raw = spark.createDataFrame(sensor_data, my_cols)

df_sensor_raw = (
    df_sensor_raw
    .withColumn(
        "timestamp",
        coalesce(
            to_date("timestamp","yyyy-MM-dd")
        )
    )
    .withColumn(
        "temp",
        col("temp").cast("double")
    )
)
df_sensor_raw.show()
df_sensor_raw.printSchema()

sensor_df_split = (
    df_sensor_raw
    .withColumn(
        "year",
        split("timestamp", "-")[0]
    )
)
sensor_df_split.show()

max_min_date =(
    df_sensor_raw
    .groupby("id")
    .agg(
        min("timestamp").alias("min_date"),
        max("timestamp").alias("max_date")
    )
)
max_min_date.show()

list_dates_df = (
    max_min_date
    .withColumn(
        "dates_list",
        explode(
            sequence("min_date", "max_date")
        )
    )
)

list_dates_df.show()