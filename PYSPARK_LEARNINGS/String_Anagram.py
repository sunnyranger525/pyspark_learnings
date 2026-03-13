import os
import urllib.request
import ssl
from struct import Struct

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

conf = SparkConf() \
    .setMaster("local[*]") \
    .set("spark.driver.host","localhost") \
    .set("spark.default.parallelism", "1")

sc = SparkContext(conf=conf)

# spark = SparkSession.builder.getOrCreate()

#spark.read.format("csv").load("data/test.txt") \
    #.toDF("Success").show(20, False)

##################🔴🔴🔴🔴🔴🔴 -> DON'T TOUCH ABOVE CODE -- TYPE BELOW ####################################

print("==========Reading Local DATA =============")
spark = SparkSession.builder.appName("Anagram").getOrCreate()
data = [
    ("listen", "silent"),
    ("spark", "parks"),
    ("hello", "world"),
    ("Triangle", "Integral")
]

from pyspark.sql.types import *
my_schema = StructType([
    StructField("str1", StringType(), True),
    StructField("str2", StringType(), True)
])

raw_string_tab = spark.createDataFrame(data, my_schema)
raw_string_tab.printSchema()
raw_string_tab.show()

string_check_df = (
    raw_string_tab
    .withColumn(
        "str1_sort",
        array_join(
            array_sort(
                split(
                    lower(col("str1")), ""
                )
            ),
            "",
            "-"
        )
    )
    .withColumn(
        "str2_sort",
        array_join(
            array_sort(
                split(
                    lower(col("str2")), ""
                )
            ),
            "", "-"
        )
    )
    .withColumn(
        "Anagram_check",
        when(
            col("str1_sort") == col("str2_sort"),
            "Anagram"
        ).otherwise("Not Anagram")
    )
    .withColumn(
        "array_col",
        array(
            col("str1"),
            col("str2"),
            col("Anagram_check")
        )
    )
    .withColumn(
        "array_count",
        array_size(col("array_col"))
    )
    .withColumn(
        "array_ele_check",
        array_contains(
            col("array_col"),
            "spark"
        )
    )
    .withColumn(
        "array_iter",
        transform(
            col("array_col"),
            lambda x : length(x)
        )
    )
    .withColumn(
        "array_dist",
        array_distinct(col("array_iter"))
    )
    .withColumn(
        "array_max",
        array_min(col("array_dist"))
    )
    .withColumn(
        "size",
        size(col("array_dist"))
    )
    .withColumn(
        "first_element",
        element_at(col("array_iter"), 1)
    )
    .withColumn(
        "slice_ele",
        slice(col("array_iter"), 1, 2)
    )
    .withColumn(
        "even_array",
        filter(
            col("array_iter"),
            lambda x : x%2 !=0
        )
    )
    .withColumn(
        "array_remove",
        array_remove(col("even_array"), 7)
    )
    .withColumn(
        "array_union",
        array_union(col("slice_ele"), col("even_array"))
    )
    .withColumn(
        "array_intersect",
        array_intersect("slice_ele", "even_array")
    )
    .withColumn(
        "array_except",
        array_except("even_array", "slice_ele")
    )
)
string_check_df.show()
