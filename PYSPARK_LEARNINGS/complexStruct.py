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
  "first_name": "Rajeev",
  "last_name": "Sharma",
  "email_address": "rajeev@ezeelive.com",
  "is_alive": true,
  "age": 30,
  "height_cm": 185.2,
  "billing_address": {
    "address": "502, Main Market, Evershine City, Evershine, Vasai East",
    "city": "Vasai Raod, Palghar",
    "state": "Maharashtra",
    "postal_code": "401208"
  },
 "date_of_birth": null
}
  
"""

df = spark.read.json(sc.parallelize([data]))
df.show()
df.printSchema()

# flatten data using select

flattendf = df.select(
    "age",
    "billing_address.address",
    "billing_address.city",
    "billing_address.postal_code",
    "billing_address.state",
    "date_of_birth",
    "first_name",
    "height_cm",
    "is_alive",
    "last_name"
)
flattendf.show()
flattendf.printSchema()

# flatten json file using withcoloumn

flattendf2 = (
    df
    .withColumn(
        "Add", expr("billing_address.address")
    )
    .withColumn(
        "city", expr("billing_address.city")
    )
    .withColumn(
        "postal_code", expr("billing_address.postal_code")
    )
    .withColumn(
        "state", expr("billing_address.state")
    )
    .drop("billing_address")
)
flattendf2.show()
flattendf2.printSchema()


