import os
import urllib.request
import ssl
from typing import final

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

conf = SparkConf()\
    .setMaster("local[*]") \
    .set("spark.driver.host","localhost") \
    .set("spark.default.parallelism", "1")

sc = SparkContext(conf=conf)

#spark.read.format("csv").load("data/test.txt") \
    #.toDF("Success").show(20, False)

##################🔴🔴🔴🔴🔴🔴 -> DON'T TOUCH ABOVE CODE -- TYPE BELOW ####################################

spark = SparkSession.builder.appName("Same_salary").getOrCreate()

data = [
    ("001", "Monika", "Arora", 100000, "2014-02-20 09:00:00", "HR"),
    ("002", "Niharika", "Verma", 300000, "2014-06-11 09:00:00", "Admin"),
    ("003", "Vishal", "Singhal", 300000, "2014-02-20 09:00:00", "HR"),
    ("004", "Amitabh", "Singh", 500000, "2014-02-20 09:00:00", "Admin"),
    ("005", "Vivek", "Bhati", 500000, "2014-06-11 09:00:00", "Admin")
]

# creating data frame

cols = ["id", "fname", "lname", "salary", "date", "dept"]

rdd = sc.parallelize(data)

print(rdd.collect()[3])

print("  ")
print("===collecting and storing value ")
person_3rd = rdd.collect()[2][1]
print(person_3rd)

rdd.foreach(print)
# converto to data frame
df = rdd.toDF(cols)
df.show()

same_salary = (
    df.groupby("salary")
    .agg(
        count("*").alias("cnt")
    )
    .select(
        col("salary")
    )
    .filter("cnt > 1")
)
same_salary.show()

final_df = (
    df.join(
        broadcast(same_salary),
        "salary",
        "inner"
    )
    .selectExpr(
        "id",
        "fname",
        "lname",
        "salary",
        "date as datetime",
        "dept"
    )
)

# final result
final_df.show()