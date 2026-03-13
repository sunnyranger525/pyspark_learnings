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
conf = SparkConf().setAppName("pyspark") \
    .setMaster("local[*]") \
    .set("spark.driver.host","localhost") \
    .set("spark.default.parallelism", "1")

sc = SparkContext(conf=conf)

spark = SparkSession.builder.getOrCreate()

#spark.read.format("csv").load("data/test.txt") \
    #.toDF("Success").show(20, False)

##################🔴🔴🔴🔴🔴🔴 -> DON'T TOUCH ABOVE CODE -- TYPE BELOW ####################################

print("==========load Multi line json =============")

data = """
{
	"Name": "Vasu",
	"Mobile": 12345678,
	"Boolean": true,
	"Pets": ["Dog", "cat"],
	"state" : "TamilNadu"

}
"""

# parse as rdd

rdd_parse = sc.parallelize([data])

df_raw = spark.read.json(rdd_parse)
df_raw.show()
df_raw.printSchema()

# flatten data
flattendf = (
    df_raw
    .selectExpr(
        "Boolean",
        "Mobile",
        "name",
        "explode(Pets)",
        "state"
    )
)
flattendf.show()
flattendf.printSchema()

# flatten with withcolumn

flattendf2 = (
    df_raw
    .withColumn(
        "Pets", expr("explode(Pets)")
    )
)
flattendf2.show()
flattendf2.printSchema()


