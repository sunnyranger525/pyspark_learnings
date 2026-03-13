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

conf = SparkConf() \
    .setMaster("local[*]") \
    .set("spark.driver.host","localhost") \
    .set("spark.default.parallelism", "1")

sc = SparkContext(conf=conf)

# spark = SparkSession.builder.getOrCreate()

#spark.read.format("csv").load("data/test.txt") \
    #.toDF("Success").show(20, False)

##################🔴🔴🔴🔴🔴🔴 -> DON'T TOUCH ABOVE CODE -- TYPE BELOW ####################################

print("========== sample hadling =============")

spark = SparkSession.builder.appName("pivote_test").getOrCreate()

data = [
    ("2025-10-01", "Electronics", 1000),
    ("2025-10-01", "Clothing", 500),
    ("2025-10-01", "Groceries", 300),
    ("2025-10-02", "Electronics", 1500),
    ("2025-10-02", "Clothing", 400),
    ("2025-10-02", "Groceries", 600)
]

cols = ["date", "category", "sales"]

df = spark.createDataFrame(data,cols)

df_fine = (
    df.withColumn(
        "date",
        to_date("date", "yyyy-MM-dd")
    )
)
df_fine.show()

# pivot functions
pivot_df =(
    df_fine
    .groupBy("date")
    .pivot("category")
    .agg(sum("sales").alias("total_sales"))
)
pivot_df.show()