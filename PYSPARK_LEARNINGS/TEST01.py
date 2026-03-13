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
print("=============== Creating Raw DataFrame =============")

rawdf = spark.read.format("csv") \
    .option("header", "true") \
    .load("df.csv")
rawdf.printSchema()
rawdf.show()

print("=====Convert into Proper data types===")
castdf = (
    rawdf
    .withColumn("id", rawdf["id"].cast("int"))
    .withColumn("amount", col("amount").cast("float"))
    .withColumn("tdate", to_date("tdate", "MM-dd-yyyy"))
)
#save method 1
castdf.write.mode("overwrite").csv(r"E:\DEV")

#save method 2
castdf.write.format("csv").mode("overwrite").save(r"E:\CERT")

#save method 3
castdf.write.format("csv").mode("overwrite") \
    .option("header", "true") \
    .option("path", r"E:\PROD").save()

#save method 4
castdf.write.mode("overwrite").csv(r"E:/DEV/csvfiles")


print("Data has been saved specified location .............")




