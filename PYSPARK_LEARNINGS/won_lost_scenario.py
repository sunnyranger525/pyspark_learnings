import os
import urllib.request
import ssl
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys
python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path
os.environ['HADOOP_HOME'] =r"C:\hadoop"
#os.environ["PATH"] += ";C:\\hadoop\\bin"
os.environ['JAVA_HOME'] = r'C:\Program Files\Eclipse Adoptium\jdk-17.0.16.8-hotspot'
######################🔴🔴🔴################################
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 pyspark-shell'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-avro_2.12:3.5.4 pyspark-shell'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4 pyspark-shell'

conf = SparkConf().setAppName("pyspark") \
    .setMaster("local[*]") \
    .set("spark.driver.host","localhost") \
    .set("spark.default.parallelism", "1")

sc = SparkContext(conf=conf)

spark = SparkSession.builder.getOrCreate()

#spark.read.format("csv").load("data/test.txt") \
    #.toDF("Success").show(20, False)

##################🔴🔴🔴🔴🔴🔴 -> DON'T TOUCH ABOVE CODE -- TYPE BELOW ####################################

print("======== Creating medals data frame=======")
data_won_lost = [
    ("01-06-2020", "Won"),
    ("02-06-2020", "Won"),
    ("03-06-2020", "Won"),
    ("04-06-2020", "Lost"),
    ("05-06-2020", "Lost"),
    ("06-06-2020", "Lost"),
    ("07-06-2020", "Won")
]

# Schema
my_schema = ["date", "status"]

# creating data frame
raw_df = spark.createDataFrame(data_won_lost, my_schema)
raw_df =(
    raw_df.
    withColumn(
        "date",
        to_date("date", "dd-MM-yyyy")
    )
)
# raw_df.show()
raw_df.printSchema()

#import windhow functions
from pyspark.sql.window import Window
date_part = Window.partitionBy().orderBy(col("date"))

# apply lag fucntions
prev_status = (
    raw_df
    .withColumn(
    "pre_state", lag("status").over(date_part)
    )
    .withColumn(
        "value",
        when(col("status") == col("pre_state"), 0)
        .otherwise(1)
    )
    .withColumn(
        "value_sum",
        sum("value").over(date_part)
    )
)
# prev_status.show()
value_spec = (
    Window
    .partitionBy("value_sum")
    .orderBy(col("date"))
    .rowsBetween(
        Window.unboundedPreceding,
        Window.unboundedFollowing
    )
)

review_df = (
    prev_status
    .withColumn("Start_date", first("date").over(value_spec))
    .withColumn("End_date", last("date").over(value_spec))
    .select(
        col("status"),
        col("start_date"),
        col("End_date")
    )
    .dropDuplicates()
)
review_df.show()

# cache the data frame,
review_df.cache()

review_df.write.format("parquet") \
    .mode("overwrite") \
    .save(r"E:/DATA/parquetfiles/output/won_data/")

# review_df.write.format("avro") \
#     .mode("overwrite") \
#     .save(r"E:/DATA/avrofiles/ouput/won_data/")

review_df.write.format("orc") \
    .mode("overwrite") \
    .save(r"E:/DATA/orcfiles/ouput/won_data/")
print("  ")
print("Data has been loaded at taget locaion......")


