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
        "row_num",
        row_number().over(spece_01)
    )
    .withColumn(
        "day_value",
        dayofmonth(col("date"))
    )
    .withColumn(
        "diff_value",
        abs(col("row_num") - col("day_value"))
    )
)
filter_worked_dates.show()

# use groupy by
worked_df_group = (
    filter_worked_dates
    .groupBy("id", "diff_value")
    .agg(
        min(col("date")).alias("start_date"),
        max(col("date")).alias("end_date"),
        count("*").alias("worked_days")
    )
    .filter(col("worked_days") >=2)
    .select("id", "start_date", "end_date", "worked_days")
    .orderBy(col("worked_days").desc())
)
worked_df_group.show()

# above method will work for very few , its not 100% correct,











