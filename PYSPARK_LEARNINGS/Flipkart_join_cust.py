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

spark = SparkSession.builder.appName("Missing_dates_fill").getOrCreate()
from pyspark.sql.types import *
from pyspark.sql.functions import explode

spark = SparkSession.builder.appName("3_months_category").getOrCreate()

customer_data = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(r"E://DATA/csvfiles/input/flipkart_customers.csv")
)

# print("loading customer csv file successfull..........")
# customer_data.printSchema()

orders_data = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(r"E://DATA/csvfiles/input/flipkart_orders.csv")
)
# print("loading orders csv file successfull............")
# orders_data.printSchema()

orderitems_data = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(r"E://DATA/csvfiles/input/flipkart_orderitems.csv")
)
# print(" loading order item csv file....")
# orderitems_data.printSchema()

product_data = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(r"E://DATA/csvfiles/input/flipkart_products.csv")
)
# print("product data loaded sucessfull........")
# product_data.printSchema()

# filter orders table 3 months

orders_data_filter = (
    orders_data
    .filter(col("order_date") >= add_months(current_date(), -3))
    .orderBy("order_date")
)
# orders_data_filter.show()

# join cust and filter orders data

customer_3_months_data = (
    customer_data
    .join(
        orders_data_filter,
        "customer_id",
        "inner"
    )
    .join(
        orderitems_data,
        "order_id",
        "inner"
    )
    .join(
        product_data,
        "product_id",
        "inner"
    )
    .orderBy("order_date")
    .select(
        "product_id",
        "customer_id",
        "customer_name",
        "order_date",
        "product_name",
        "category"
    )
)
# customer_3_months_data.show()

# get window library,
from pyspark.sql.window import *

wind_spec = (
    Window
    .partitionBy(
        "category",
        "customer_id"
    )
    .orderBy("customer_id")
)

count_value = (
    customer_3_months_data
    .withColumn(
        "value_count",
        count("*").over(wind_spec)
    )
    .filter(col("value_count") >= 3)
)
count_value.show()

count_value.explain()

value = input("Enter any values......")