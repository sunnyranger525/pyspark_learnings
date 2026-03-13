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

print("==========Loading CSV file =============")

from pyspark.sql.functions import *

rawdf = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("df.csv")
rawdf.show()
rawdf.printSchema()
df1 = rawdf.withColumn(
    "status", when(
        col("spendby") == "cash", 1
    )
    .when(
        col("spendby") == "credit", 2
    )
    .otherwise(0)
) \
    .withColumn(
    "tdate", to_date("tdate", "MM-dd-yyyy")
)
df1.show()
df1.printSchema()
print ("=======find number of partitions======")
print(df1.rdd.getNumPartitions())

df1 = df1.repartition(4, col("id"))
print(df1.rdd.getNumPartitions())
# save method 1 csv file
# having forward slash, it create directory auto if not there
df1.write.mode("overwrite").csv(r"E:\DEV\cash")

# save method 2 csv file
df1.coalesce(2).write.format("csv").mode("overwrite") \
    .option("header", "true") \
    .save(r"E:/DEV/cash/csv1/")
#save method 3 csv file
df1.coalesce(3).write.format("csv").mode("overwrite") \
    .option("header", "true") \
    .option("path", r"E:/DEV/cash/csv2/") \
    .save()

print("Saved Successfull................")

print("=============Filter Cash Data ===============")
df2 = df1.filter(col("spendby") == "cash") \
    .filter("amount > 200 ")
df2.show()


print("===========Filter Credit Data ===============")
df3 = df1.filter(df1["spendby"] == "credit")
df3.show()

print("============Unit Data Cash and Credit ========")
df4 = df2.union(df3)
df4.show()






