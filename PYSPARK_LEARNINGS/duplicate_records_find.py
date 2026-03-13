import os
import urllib.request
import ssl
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys

from pyspark.sql.types import StructType, StructField

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

spark = SparkSession.builder.appName("Duplicate_find").getOrCreate()

data = [
    (1, 1, 100, "2025-08-01"),
    (2, 1, 100, "2025-08-01"),
    (3, 2, 200, "2025-08-02"),
    (4, 2, 200, "2025-08-02"),
    (5, 3, 300, "2025-08-02"),
    (6, 4, 400, "2025-08-02")
]
from pyspark.sql.types import *
my_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("custid", IntegerType(), True),
    StructField("amount", IntegerType(), True),
    StructField("transdate", StringType(), True)
])

trans_df = spark.createDataFrame(data, my_schema)
trans_df.printSchema()
trans_df = trans_df.withColumn("transdate", to_date("transdate", "yyyy-MM-dd"))
trans_df.show()
trans_df.printSchema()


# rownumber
from pyspark.sql.window import Window
wind_spec = Window.partitionBy("custid", "amount", "transdate").orderBy(lit(1))
trans_df_row = trans_df \
    .withColumn("row_num", row_number().over(wind_spec)) \
    .withColumn("cnt_num", count("*").over(wind_spec))
trans_df_row.show()

#Groupby method
trans_df_group = trans_df.groupBy("custid", "amount", "transdate") \
    .agg(count("*").alias("cnt_dup")) \
    .filter(col("cnt_dup") > 1)
trans_df_group.show()

# do join
join_left_anti = (
    trans_df_row
    .join(
        trans_df_group,
        ["custid", "amount", "transdate"],
        "inner"
    )
    .orderBy(col("id").desc())
    .select(
        trans_df_row["id"],
        trans_df_row["custid"],
        trans_df_row["amount"],
        trans_df_row["transdate"]
    )
    .orderBy(col("id").asc())
)
join_left_anti.show()
