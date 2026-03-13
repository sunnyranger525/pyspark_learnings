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

spark = SparkSession.builder.appName("Consective_scenario").getOrCreate()
from pyspark.sql.types import *

data = [
    (1, "2025-11-01"),
    (1, "2025-11-02"),
    (1, "2025-11-03"),
    (1, "2025-11-05"),
    (2, "2025-11-01"),
    (2, "2025-11-03"),
    (2, "2025-11-04")
]
#creating Schema
cols = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("date", StringType(), True)
    ]
)

# Creating DataFrame

df1 = spark.createDataFrame(data, cols)
df1 = df1.withColumn("date", to_date("date", "yyyy-MM-dd"))
df1.show()
df1.printSchema()

# Aim to find consective days
# go with lag method to differentiate
from pyspark.sql.window import *

win_spec = Window.partitionBy("id").orderBy(col("date").asc())

df2_lag = (
    df1.withColumn(
        "pre_date",
        lag("date", 1).over(win_spec)
    )
    .withColumn(
        "date_diff_days",
        date_diff(col("date"), col("pre_date"))
    )
    .withColumn(
        "date_df_days",
        coalesce(col("date_diff_days"), lit(0))
    )
    .withColumn(
        "change_status",
        when(col("date_df_days") !=1, 1)
        .otherwise(0).cast("int")
    )
    .withColumn(
        "running_sum",
        sum(col("change_status")).over(win_spec)
    )
)
df2_lag.show()
df2_lag.printSchema()

count_consective = (
    df2_lag
    .groupby(
        "id",
        "running_sum"
    )
    .agg(count("*").alias("cnt"))
    .filter(col("cnt") > 1)
)
count_consective.show()

# spark SQL,

df1.createOrReplaceTempView("Tab_01")

df_sql_01 = spark.sql("""
    with pre_tab as (
        select id, date,
            lag(date, 1) over(
                partition by id 
                order by date asc
            ) as pre_date
        from Tab_01
    ),
    diff_tab as (
        select id, date,
            date_diff(date, pre_date) as days_dff
        from pre_tab
    ),
    status_tab as (
        select *,
            sum (case
                when days_dff != 1 then 1
                when days_dff is null then 1
                else 0
            end) over (
                partition by id order by date )
                as Status
        from diff_tab
    )
    select id, status, count(*) as cnt from status_tab
    group by id, status
    having cnt > 1
""").show()
