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

spark = SparkSession.builder.appName("Consective_scenario_03").getOrCreate()
from pyspark.sql.types import *

data = [
    (101, "02-01-2024", "N"),
    (101, "03-01-2024", "Y"),
    (101, "04-01-2024", "N"),
    (101, "07-01-2024", "Y"),
    (102, "01-01-2024", "N"),
    (102, "02-01-2024", "Y"),
    (102, "03-01-2024", "Y"),
    (102, "04-01-2024", "N"),
    (102, "05-01-2024", "Y"),
    (102, "06-01-2024", "Y"),
    (102, "07-01-2024", "Y"),
    (103, "01-01-2024", "N"),
    (103, "04-01-2024", "N"),
    (103, "05-01-2024", "Y"),
    (103, "06-01-2024", "Y"),
    (103, "07-01-2024", "N")
]
# define schema
my_schema = ["id", "date", "status_flag"]

# create dataframe based on data and schema
raw_login = spark.createDataFrame(data, my_schema)
raw_login.show()
raw_login.printSchema()

# convert date string to date datatype
login_df = (
    raw_login
    .withColumn(
        "date", to_date(col("date"), "dd-MM-yyyy")
    )
)
login_df.printSchema()

# find consective worked dates in dates at least two days
# filter active worked dates
# apply window funtion based on id and order dates

from pyspark.sql.window import Window
spece_01 = Window.partitionBy("id").orderBy(col("date"))

filter_worked_dates = (
    login_df
    .filter(col("status_flag") ==  "Y")
    .withColumn(
        "pre_date",
        lag("date").over(spece_01)
    )
    .withColumn(
        "date_diff",
        date_diff(col("date"), col("pre_date"))
    )
    .withColumn(
        "status_flag",
        when(
            col("date_diff") == 1, 0
        ).otherwise(2)
    )
    .withColumn(
        "run_sum",
        sum(col("status_flag")).over(spece_01)
    )
    .select(
        "id",
        "date",
        "run_sum"
    )
    .groupBy("id", "run_sum")
    .agg(
        min("date").alias("min"),
        max("date").alias("max"),
        count("*").alias("cnt_days")
    )
    .filter(col("cnt_days") >=2)
    .select(
        "id",
        expr("min as min_date"),
        expr("max as max_date"),
        expr("cnt_days as days")
    )
)
filter_worked_dates.show()

# other two days diff data , find
filter_two_days = (
    login_df
    .filter(col("status_flag") ==  "Y")
    .withColumn(
        "pre_date",
        lag("date").over(spece_01)
    )
    .withColumn(
        "date_diff",
        date_diff(col("date"), col("pre_date"))
    )
    .withColumn(
        "status_flag",
        when(
            col("date_diff") == 4, 0
        ).otherwise(1)
    )
    .withColumn(
        "run_sum",
        sum(col("status_flag")).over(spece_01)
    )
)
filter_two_days.show()