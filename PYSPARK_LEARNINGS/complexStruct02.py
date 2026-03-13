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

data = """
{
	"id": "000",
	"type": "donut",
	"name": "Non cream",
	"image": {
		"url": "images/0001.jpg",
		"width": 200,
		"height": 200
	},
	"thumbnail": {
		"url": "images/thumbnails/0001.jpg",
		"width": 33,
		"height": 33
	}
}
"""
# json data is local in driver program

# create RDD

rdd1 = sc.parallelize([data])

df = spark.read.json(rdd1)
df.show()
df.printSchema()

# Flatten data using select

selflatten  = (
    df
    .select(
        "id",
        "image.height",
        "image.url",
        "image.width",
        "name",
        "thumbnail.height",
        "thumbnail.url",
        "thumbnail.url",
        "thumbnail.width",
        "type"
    )
)
selflatten.show()
selflatten.printSchema()

# flatten json data using withcolumn

flattendf2 = (
    df
    .withColumn(
        "height", expr("image.height")
    )
    .withColumn(
        "url", expr("image.url")
    )
    .withColumn(
        "width", expr("image.width")
    )
    .withColumn(
        "height_thum", expr("thumbnail.height")
    )
    .withColumn(
        "thum_url", expr("thumbnail.url")
    )
    .withColumn(
        "thum_width", expr("thumbnail.width")
    )
    .drop("image", "thumbnail")
)

flattendf2.show()
flattendf2.printSchema()


