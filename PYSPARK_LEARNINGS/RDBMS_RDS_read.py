import os
import urllib.request
import ssl
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys

from urllib3 import request

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path
os.environ['HADOOP_HOME'] =r"C:\hadoop"
os.environ['JAVA_HOME'] = r'C:\Program Files\Eclipse Adoptium\jdk-17.0.16.8-hotspot'
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--jars file:/E:/connectors/mysql/mysql-connector-j-8.0.33/mysql-connector-j-8.0.33.jar pyspark-shell"
)
######################🔴🔴🔴################################
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 pyspark-shell'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-avro_2.12:3.5.4 pyspark-shell'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4 pyspark-shell'

conf = SparkConf().setAppName("pyspark") \
    .setMaster("local[*]") \
    .set("spark.driver.host","localhost") \
    .set("spark.default.parallelism", "1")

sc = SparkContext(conf=conf)

# spark = SparkSession.builder.getOrCreate()

#spark.read.format("csv").load("data/test.txt") \
    #.toDF("Success").show(20, False)

##################🔴🔴🔴🔴🔴🔴 -> DON'T TOUCH ABOVE CODE -- TYPE BELOW ####################################

print("==========Reading URL Json file format =============")

spark = SparkSession.builder.appName("RDBMS_Read")\
    .getOrCreate()

print(spark.sparkContext._jsc.sc().listJars())
rds_user = "*******"
rds = "************"
dbtable_name = "customers"
endpoint = r"*****************"
jdbc_url = (
    f"jdbc:mysql://{endpoint}:3306/statidb"
    "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC"
)
jdbc_url1 = (
    f"jdbc:mysql://{endpoint}:3306/statidb"
)
driver_conn = "com.mysql.cj.jdbc.Driver"

cust_df =(
    spark.read.format("jdbc")
    .option("url", jdbc_url1)
    .option("user", rds_user)
    .option("password", rds)
    .option("dbtable", dbtable_name)
    .option("driver", driver_conn)
    .load()
)


(
    cust_df.write.format("jdbc")
    .option("url", jdbc_url)
    .option("user", rds_user)
    .option("password", rds)
    .option("dbtable", dbtable_name)
    .option("driver", driver_conn)
    .mode("append")
    .option("batchsize", "100")
    .save()
)
print("Write happend sucessfull..........")

os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--jars file:/"
)








