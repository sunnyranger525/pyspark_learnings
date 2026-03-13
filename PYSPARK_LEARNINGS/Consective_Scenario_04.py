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

spark = SparkSession.builder.appName("Consective_scenario").getOrCreate()
from pyspark.sql.types import *

data = [
    (1, "2025-11-01"),
    (1, "2025-11-02"),
    (1, "2025-11-03"),
    (1, "2025-11-05"),
    (2, "2025-11-01"),
    (2, "2025-11-03"),
    (2, "2025-11-04")
]
schema_df = StructType([
    StructField("id", IntegerType(), True),
    StructField("date", StringType(), True)
])

# Creating Raw Data Frame
raw_df = spark.createDataFrame(data, schema_df)
raw_df.show()
raw_df.printSchema()

# convert date string to date datatype
# add row number and extract date as well
from pyspark.sql.window import Window
spece_01 = Window.partitionBy("id").orderBy(col("date"))

df = (
    raw_df
    .withColumn(
        "date",
        to_date(col("date"), "yyyy-MM-dd")
    )
    .withColumn(
        "pre_date",
        lag("date").over(spece_01)
    )
    .withColumn(
        "cnt_diff",
        date_diff("date", "pre_date")
    )
    .withColumn(
        "value",
        when(
            col("cnt_diff") == 1, 0
        ).otherwise(1)
    )
)
df.show()
# define window spec
spec_02 = Window.partitionBy("id") \
    .orderBy("date")

df2 = (
    df
    .withColumn(
        "run_sum",
        sum(col("value")).over(spec_02)
    )
    .groupBy("id", "run_sum")
    .agg(
        min("date").alias("start_date"),
        max("date").alias("end_date"),
        count("*").alias("con_days")
    )
    .filter(col("con_days") > 1)
    .select("id", "start_date", "end_date", "con_days")
)
df2.show()

# perform run sum based on status flag
df3 = (
    df
    .withColumn(
        "run_sum",
        sum(col("value")).over(spec_02)
    )
)
df3.show()

# define window and find consecutive days
spec_03 = Window.partitionBy("id", "run_sum")
df4 =(
    df3
    .withColumn(
        "cnt_days",
        count("*").over(spec_03)
    )
    .filter(col("cnt_days") > 1)
    .select("id", "date", expr("cnt_days as days"))
    .orderBy(col("id"), col("date"))
)
df4.show()
