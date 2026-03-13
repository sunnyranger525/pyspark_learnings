import os
import urllib.request
import ssl
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys

from urllib3 import request

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

print("==========Reading URL Json file format =============")

json_data = {
    "results": [
        {
            "user": {
                "gender": "female",
                "name": {
                    "title": "miss",
                    "first": "elisa",
                    "last": "gonzalez"
                },
                "location": {
                    "street": "3480 avenida de américa",
                    "city": "santa cruz de tenerife",
                    "state": "comunidad valenciana",
                    "zip": 81968
                },
                "email": "elisa.gonzalez@example.com",
                "phone": "916-525-581",
            }
        },
    ]
}
access_1 = json_data["results"][0]["user"]
print(access_1)








