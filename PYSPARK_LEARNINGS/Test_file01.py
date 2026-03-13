import os
import urllib.request
import ssl
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys
python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path
os.environ['HADOOP_HOME'] ="hadoop"
os.environ['JAVA_HOME'] = r'C:\Program Files\Eclipse Adoptium\jdk-17.0.16.8-hotspot'
conf = (
    SparkConf().setAppName("pyspark")
    .setMaster("local[*]")
    .set("spark.driver.host","localhost")
    .set("spark.default.parallelism", "1")
)
sc = SparkContext(conf=conf)
spark = SparkSession.builder.getOrCreate()
spark.read.format("csv").load("data/test.txt").toDF("Success").show(20, False)
##################🔴🔴🔴🔴🔴🔴 -> DONT TOUCH ABOVE CODE -- TYPE BELOW ####################################
print()
