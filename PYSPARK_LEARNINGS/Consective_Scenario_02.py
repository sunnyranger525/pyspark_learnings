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
schema_df = StructType([
    StructField("id", IntegerType(), True),
    StructField("date", StringType(), True)
])

# Creating Raw Data Frame
raw_df = spark.createDataFrame(data, schema_df)
raw_df.show()
raw_df.printSchema()

# Creating Window Partition

# Creating and applying Spec
from pyspark.sql import Window

wind_spec = Window.partitionBy(col("id")).orderBy(col("date"))

main_df = (
    raw_df
    .withColumn(
        "date",
        to_date("date", "yyyy-MM-dd")
    )
    .withColumn(
        "prev_date",
        lag(col("date")).over(wind_spec)
    )
    .withColumn(
        "days",
        date_diff("date", "prev_date")
    )
    .withColumn(
        "status",
        when(col("days") !=1, 1)
        .when(col("days").isNull(), 1)
        .otherwise(0).cast("int")
    )
    .withColumn(
        "running_sum",
        sum(col("status")).over(wind_spec)
    )
)
main_df.show()
main_df.printSchema()
win_spec2 = Window.orderBy("id", "date")
df_final = (
    main_df
    .withColumn(
        "prev_value",
        lag(col("running_sum")).over(win_spec2)
    )
    .withColumn(
        "grp",
        when(
            col("running_sum") == col("prev_value"),
            0
        )
        .otherwise(1)
    )
    .withColumn(
        "sum_grp",
        sum(col("grp")).over(win_spec2)
    )
)
df_final.show()
df_final.printSchema()

win_spec3 = Window.partitionBy("sum_grp")

result_df = (
    df_final
    .withColumn(
        "cnt",
        count("*").over(win_spec3)
    )
    .filter(col("cnt") > 1)
    .selectExpr(
        "id",
        "date",
        "cnt"
    )
)

result_df.show()

# SparkSQL Method

raw_df.createOrReplaceTempView("consective_table")

final_con_df = spark.sql("""
with prev_tab as (
    select 
        id, 
        cast(date as date), 
        lag(date, 1) over (partition by id order by date) as prev_date
    from consective_table
    ),
    diff_tab as (
        select *, date_diff(date, prev_date) as days from prev_tab
    ),
    change_state_tab as (
        select *, 
            case
                when days != 1 then 1
                when days is null then 1
                else 0
            end as status
        from diff_tab         
    ),
    running_sum_tab as (
        select *,
            sum(status)
                over (partition by id order by date
                rows between unbounded preceding and current row)
                as run_sum
        from change_state_tab
    ),
    group_count as (
        select *,
            count(*) over (
                partition by id, run_sum order by run_sum
                ) as count_value
        from running_sum_tab
    )
    select id, date, count_value from group_count
    where count_value > 1
""").show()







