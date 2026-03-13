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

print("==========Reading CSV File =============")
spark = SparkSession.builder.appName("CSV_File_Read").getOrCreate()

try:
    sale_df = spark.read.csv(
        r"E://DATA/csvfiles/input/sales.csv",
        header= True, inferSchema=True
    )
except Exception as e:
    print(f"path not found, please check")

sale_df.printSchema()
sale_df.show()
# Define Window functions
from pyspark.sql.window import Window
win_spec1 = Window.orderBy(lit(1))

# rand method to add salt key
rand_num_df = (
    sale_df
    .withColumn(
        "salt",
        floor(rand() * 10)
    )
)
rand_num_df.show()
rand_num_group = (
    rand_num_df
    .groupBy("salt").agg(
        count("*").alias("cnt_salt_key")
    )
    .orderBy(col("salt"))
)
rand_num_group.show(30)

# hash method to add salt key
hash_salt_key = (
    sale_df
    .withColumn(
        "salt",
        pmod(hash("*"), 10)
    )
)
hash_cnt_check = (
    hash_salt_key
    .groupBy("salt").agg(
        count("*").alias("salt_key_cnt")
    )
    .orderBy(col("salt"))
)
hash_cnt_check.show(15)

# zip with index method to add salt key

zip = (
    sale_df.rdd.zipWithIndex()
)
# zip.foreach(print)

from pyspark.sql.types import Row
# help(Row)
zip_merge_salt_key = (
    zip.map(lambda x : Row(**x[0].asDict(), salt=(x[1]%10)))
)
# zip_merge_salt_key.foreach(print)
salt_key_df = (
    spark.createDataFrame(zip_merge_salt_key)
)
# salt_key_df.show()
# count salt key based on zip with index
count_salt_zip_key = (
    salt_key_df.groupBy("salt").agg(
        count("*").alias("salt_cnt")
    )
)
count_salt_zip_key.show(20)
# creating Row
row_create = (
    [
        Row(id = 1, name = 'sam', salary = 1000),
        Row(id = 2, name = "hima", salary = 1500)
    ]

)
creat_df_row = spark.createDataFrame(row_create)
creat_df_row.show()
# partition on rand or salt key
from pyspark.sql.window import Window

# creating smaller table country ,
country_df = (
    rand_num_df
    .select("country").distinct()
)

# Adding salt key and explode smaller table
country_df_rand = (
    country_df
    .withColumn(
        "salt_key",
        lit([x for x in range(5)])
    )
)
country_df_rand.show()

# join_df_salt = (
#     rand_num_df.alias("m")
#     .filter(col("country") == "USA")
#     .join(
#         broadcast(country_df_rand).alias("c"),
#         (rand_num_df["country"] == country_df_rand["country"])
#         &
#         (rand_num_df["salt"] == country_df_rand["salt_key"]),
#          "inner"
#     )
# )
# join_df_salt.show()
