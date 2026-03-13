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

print("==========Reading URL Json file =============")

spark = SparkSession.builder.appName("meta-_data").getOrCreate()

meta_data = [
    (1, "p1", "2025-11-12"),
    (2, "p1", "2025-11-13"),
    (3, "p1", "2025-11-14"),
    (1, "p2", "2025-11-12"),
    (2, "p2", "2025-11-13")
]
cols = ["id", "pipeline", "date"]

df_meta = spark.createDataFrame(meta_data, cols)
df_meta = df_meta.withColumn("date", to_date("date", "yyyy-MM-dd"))
df_meta.show()
df_meta.printSchema()

process_date = df_meta.filter(col("pipeline") == "p2") \
    .agg(max("date").alias("max_date")) \
    .collect()[0][0]
print(process_date)

id = df_meta.filter(col("pipeline") == "p2")\
    .orderBy(col("date").desc()) \
    .collect()[0][0]
print(id)

iter = id + 1
print(iter)

pipeline_name = df_meta.filter(col("pipeline") == "p2") \
    .orderBy(col("date").desc()) \
    .collect()[0][1]
print(pipeline_name)

df_filter = df_meta.filter(col("date") > process_date)
df_filter.show()
from datetime import timedelta
new_date = process_date + timedelta(days =1)

meta_data_append = [
    (iter, pipeline_name, new_date)
]
cols_meta = ["id", "pipeline", "date"]
meta_df_ite = (
    spark.createDataFrame(meta_data_append, cols_meta)
)

final_df = (
    df_meta.union(meta_df_ite)
)
final_df.show()

final_var = final_df.filter(col("pipeline") == "p1") \
    .orderBy(col("date").desc()).collect()
for var in final_var:
    id_iterr = var["id"]
    line_name = var["pipeline"]
    pro_date = var["date"]
meta_data = [id_iterr, line_name, pro_date]
print(meta_data)












