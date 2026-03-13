import os
import urllib.request
import ssl
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys

from pyspark.sql.functions import to_date

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

print("======== Creating medals data frame=======")
data_won_lost = [
    ("17-06-2020", "Won"),
    ("18/06/2020", "Won"),
    ("2020/06/19", "Won"),
    ("06/20/2020", "Lost"),
    ("21-06-2020", "Lost"),
    ("22-06-2020", "Lost"),
    ("23-06-2020", "Won")
]

cols = [ "date", "status"]
print("Creating Data Frame ..........")
data_won = spark.createDataFrame(data_won_lost, cols)
data_won.printSchema()

date_formates = (
    data_won
    .withColumn(
        "date_format",
        coalesce(
            expr("try_to_timestamp(date, 'yyyy-MM-dd')"),
            expr("try_to_timestamp(date, 'dd/MM/yyyy')"),
            expr("try_to_timestamp(date, 'dd-MM-yyyy')"),
            expr("try_to_timestamp(date, 'yyyy/MM/dd')"),
            expr("try_to_timestamp(date, 'MM/dd/yyyy')")
        )
    )
    .withColumn("date_new", to_date(col("date_format")))
)
date_formates.show()
date_formates.printSchema()

date_convert_df = (
    data_won
    .withColumn(
        "date_format",
        when(
            expr("try_to_timestamp(date, 'yyyy-MM-dd')").isNotNull(),
            expr("try_to_timestamp(date, 'yyyy-MM-dd')")
        )
        .when(
            expr("try_to_timestamp(date, 'dd/MM/yyyy')").isNotNull(),
            expr("try_to_timestamp(date, 'dd/MM/yyyy')")
        )
        .when(
            expr("try_to_timestamp(date, 'dd-MM-yyyy')").isNotNull(),
            expr("try_to_timestamp(date, 'dd-MM-yyyy')")
        )
        .when(
            expr("try_to_timestamp(date, 'yyyy/MM/dd')").isNotNull(),
            expr("try_to_timestamp(date, 'yyyy/MM/dd')")
        )
        .when(
            expr("try_to_timestamp(date, 'MM/dd/yyyy')").isNotNull(),
            expr("try_to_timestamp(date, 'MM/dd/yyyy')")
        )
    )
)
date_convert_df.show()





