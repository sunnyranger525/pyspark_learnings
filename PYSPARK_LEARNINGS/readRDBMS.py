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

#spark = SparkSession.builder.getOrCreate()

#spark.read.format("csv").load("data/test.txt") \
    #.toDF("Success").show(20, False)

##################🔴🔴🔴🔴🔴🔴 -> DON'T TOUCH ABOVE CODE -- TYPE BELOW ####################################

# Spark Session
spark = SparkSession.builder \
    .appName("ReadRDBMS") \
    .config("spark.jars", r"E:\connector\mysql") \
    .getOrCreate()

print("==========Reading from RDBMS =============")

# created jdbc url
jdbc_url = "jdbc:mysql://localhost:3303/samsudb"

# table name
table_name = "employee"

# Accound properties
properties_read = {
    "read" : "root",
    "password" : "******",
    "driver" : "com.mysql.cj.jdbc.Driver"
}

#reading file in spark
rdbdf = spark.read \
    .jdbc(
    url = jdbc_url,
    table = table_name,
    properties = properties_read,
)

rdbdf.show()

# other method to read from RDMBS

readdf = spark.read.format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/samsudb") \
    .option("dbtable", "employee") \
    .option("user", "root") \
    .option("password", "******") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .load()

print(" Json file has been saved ..............")

# performance tuning method to read from RDMBS

readdf = spark.read.format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/samsudb") \
    .option("dbtable", "employee") \
    .option("user", "root") \
    .option("password", "******") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("partitonColumn", "id") \
    .option("lowerBound", 1) \
    .option("upperBound", 50000) \
    .option("numPartitions", 10) \
    .load()

print(" Json file has been saved ..............")

# if not awar about min and max id ,
"""
follow this method
"""
# import mysql.connector
#
# connection = mysql.connector.connect(
#     host = "localhost",
#     user = "root",
#     password = "******",
#     database = "samsudb"
# )
# cursor = connection.cursor()
#
# cursor.execute("select min(id), max(id) from employee")
# min_id, max_id = cursor.fetchone()
# connection.close()





