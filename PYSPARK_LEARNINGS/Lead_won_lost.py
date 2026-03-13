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

print("======== Creating medals data frame=======")
data_won_lost = [
    ("01-06-2020", "Won"),
    ("02-06-2020", "Won"),
    ("03-06-2020", "Won"),
    ("04-06-2020", "Lost"),
    ("05-06-2020", "Lost"),
    ("06-06-2020", "Lost"),
    ("07-06-2020", "Won")
]

cols = [ "date", "status"]
print("Creating Data Frame ..........")
data_won = spark.createDataFrame(data_won_lost, cols)
data_won.printSchema()
print(data_won.schema.fieldNames())
#data_won.show()
# convert date Schema into date type
won_fine_schema = (
    data_won
    .withColumn(
        "date",
        to_date(col("date"),"dd-MM-yyyy")
    )
)
won_fine_schema.printSchema()

from pyspark.sql.window import Window
lead_part = Window.orderBy(col("date"))
lead_won_data = (
    won_fine_schema
    .withColumn(
        "lead_status",
        lag(col("status"),1).over(lead_part)
    )
    .withColumn(
        "index_cnt",
        when(
            col("status") == col("lead_status"),
            0
        )
        .otherwise(1)
    )
    .withColumn(
        "running_sum",
        sum("index_cnt").over(lead_part)
    )
)
# lead_won_data.show()

run_part = (
    Window.partitionBy("running_sum")
    .orderBy(col("running_sum"))
    .rowsBetween(
        Window.unboundedPreceding,
        Window.unboundedFollowing
    )
)
findal_result = (
    lead_won_data
    .withColumn(
        "start-date",
        min("date").over(run_part)
    )
    .withColumn(
        "max_date",
        max("date").over(run_part)
    )
    .select(
        "status",
        "start-date",
        "max_date"
    )
    .distinct()
)
findal_result.show()



