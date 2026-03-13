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

sensor_date = [
    ("S1", "2025-11-01", 30.2),
    ("S1", "2025-11-02", 31.1),
    ("S1", "2025-11-04", 29.8),
    ("S2", "2025-11-01", 25.4),
    ("S2", "2025-11-03", 26.0)
]

my_cols = StructType(
    [
        StructField("id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("temp", StringType(), True)
    ]
)
sensor_df= spark.createDataFrame(sensor_date, my_cols)
sensor_df.show()
sensor_df.printSchema()

sensor_clean_df = (
    sensor_df
    .withColumn("timestamp", to_date("timestamp", "yyyy-MM-dd"))
    .withColumn("temp", col("temp").cast("double"))
)
sensor_clean_df.show()
sensor_clean_df.printSchema()

find_min_max_date = (
    sensor_df
    .groupby("id")
    .agg(
        min("timestamp").alias("min_date"),
        max("timestamp").alias("max_date")
    )
)
find_min_max_date.show()
find_min_max_date.printSchema()

series_df_date = (
    find_min_max_date
    .withColumn(
        "min_date",
        to_date("min_date", "yyyy-MM-dd"))
    .withColumn(
        "max_date",
        to_date("max_date", "yyyy-MM-dd")
    )
    .withColumn(
        "dates_series",
        explode(sequence("min_date", "max_date"))
    )
    .selectExpr(
        "id",
        "dates_series as timestamp"
    )
)
series_df_date.show()

# final dataframe

result_df = (
    series_df_date
    .join(
        sensor_clean_df,
        ["id", "timestamp"],
        "left"
    )
    .orderBy("id", "timestamp")
    .withColumn(
        "temp",
        coalesce("temp", lit(0))
    )
)
result_df.show()

# extract dates using split and creat new coloum

extract_date_df = (
    result_df
    .withColumn(
        "year",
        split("timestamp","-")[0]
    )
    .withColumn(
        "month",
        split("timestamp", '-')[1]
    )
    .withColumn(
        "day",
        split("timestamp", "-")[2]
    )
)
extract_date_df.show()