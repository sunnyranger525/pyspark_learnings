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

print("==========Creating Udf functions =============")

# Raw Data

data = [
    (1, "John", 60000),
    (2, "Mary", 45000),
    (3, "Sam", 80000)
]

cols = ["id", "name", "salary"]

rawdf = spark.createDataFrame(data, cols)
rawdf.show()

# calculate bonus for each employee 10 % and 5%
# create Functions
from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType, StringType


def sal_bonus(sal):
    if sal >= 50000:
        return sal*0.1
    else:
        return sal*0.05

sal_bounus = udf(sal_bonus, DoubleType())

sal_bonus_df = (
    rawdf
    .withColumn(
        "Bonus", sal_bounus(col("salary"))
    )
)
sal_bonus_df.show()

def total_sal(sal1, bonus1):
    return sal1+bonus1
total_sal_udf = udf(total_sal, DoubleType())

total_sal_df = (
    sal_bonus_df
    .withColumn(
        "Total_sal",
        total_sal_udf(col("salary"), col("Bonus"))
    )
)
total_sal_df.show()

def grade_sal(sal):
    if sal > 50000:
        return "A"
    else:
        return "B"
udf_band_sal = udf(grade_sal, StringType())

sal_band_df =(
    total_sal_df
    .withColumn(
        "Band",
        udf_band_sal(col("Total_sal"))
    )
)
sal_band_df.show()