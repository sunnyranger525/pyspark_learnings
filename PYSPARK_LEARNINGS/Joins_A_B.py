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

print("==========Reading URL Json file =============")

spark = SparkSession.builder.appName("Join_A_b").getOrCreate()
from pyspark.sql.types import *

data_a = [
    (0,),
    (1,),
    (0,)
]
cols_a = ["id"]
df_a = spark.createDataFrame(data_a, cols_a)


data_b = [
    (0,),
    (1,),
    (0,)
]

cols_b = ["id"]

df_b = spark.createDataFrame(data_b, cols_b)

join_inner = df_a.join(df_b,
                       df_a["id"] == df_b["id"],
                       "inner"
                       )

print("Inner Join")
join_inner.show()

print("left join")
join_left = df_a.join(
    df_b,
    df_a["id"] == df_b["id"],
    "left"
)

join_left.show()

print("right join")
join_right = df_a.join(
    df_b,
    df_a["id"] == df_b["id"],
    "right"
)
join_right.show()

print(join_right.schema)
print(join_right.columns)
print(join_right.schema.fieldNames())
print(join_right.schema.fields)

dict_convert = {
     field.name : field.dataType.simpleString() for field in join_right.schema.fields
}
print(dict_convert)
list_cols =  [f.name for f in join_right.schema.fields]
print(list_cols)

value = input("Enter something to exit :>>: ")





