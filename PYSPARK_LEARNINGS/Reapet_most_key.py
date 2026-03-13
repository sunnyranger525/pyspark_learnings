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

print("==========Reading file =============")

spark = SparkSession.builder.appName("most_repeat").getOrCreate()

input_list  = [
    2,3,4,4,4,5,5,57,7,8,8,89,10,10
]
input_data = [(x,) for x in input_list]

df = spark.createDataFrame(input_data, ["key"])
df.show()
#Scenario -1 : Finding most repeated key
#pyspark Method
df_repeat = (df.groupBy("key")
             .agg(
                count("*").alias("cnt_key")
            )
             .orderBy(col("cnt_key").desc()).limit(1)
        )
df_repeat.show()

#My sql
df.createOrReplaceTempView("df_tab")
tab_most_repeat = spark.sql(
    """
    select key, count(key) as key_count from df_tab
    group By key
    order by key_count desc
    limit 1
    """
).show()

# Finding duplicate keys
df_dup = (
    df.groupBy("key") \
    .agg(
        count("*").alias("key_count")
    )
    .filter(col("key_count") > 1)
)
df_dup.show()

# collect function
df_collect = df_dup.collect()[1][0]
print(df_collect)







