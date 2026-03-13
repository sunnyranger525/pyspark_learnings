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

spark = SparkSession.builder.getOrCreate()

#spark.read.format("csv").load("data/test.txt") \
    #.toDF("Success").show(20, False)

##################🔴🔴🔴🔴🔴🔴 -> DON'T TOUCH ABOVE CODE -- TYPE BELOW ####################################

print("========== Null Handling =============")

from pyspark.sql.types import *

data = [
    Row(name="Sam", age=30, city=None),
    Row(name="Hima", age=None, city="Bangalore"),
    Row(name=None, age=25, city="Hyderabad"),
]

df = spark.createDataFrame(data)

df.show()

# fill null data with standard
# fillna

df_fillna = df.fillna("NA")
df_fillna.show()

# filling null data based on specific cols
# na.fill

df_na_fill = df.na.fill({
    "name" : "Not defined",
    "city" : "Not Enter"
})
df_na_fill.show()

# drop null data entire dataframe
# dropna
df_dropna = df.dropna()
df_dropna.show()

# drop null data specific cols
# na.drop

df_na_drop = df.na.drop(subset= "city")
df_na_drop.show()





