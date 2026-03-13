import os
import urllib.request
import ssl
from dataclasses import field

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys

from pyspark.sql.types import StructType, StructField, StringType, DateType, TimestampType

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

print("==========loading Sensor Data =============")

spark = SparkSession.builder.appName("Sensor_Read").getOrCreate()

sensor_data = [
    ('sensor_1', '2025-11-10 21:15:00', 101.2, 'OK'),
    ('sensor_2', '2025-11-10 21:10:00', 99.8, 'OK'),
    ('sensor_3', '2025-11-10 21:05:00', 102.4, 'FAIL'),
    ('sensor_1', '2025-11-10 21:00:00', 100.6, 'OK'),
    ('sensor_2', '2025-11-10 20:55:00', 98.9, 'OK'),
    ('sensor_3', '2025-11-10 20:50:00', 105.1, 'FAIL'),
    ('sensor_1', '2025-11-10 20:45:00', 99.3, 'OK'),
    ('sensor_2', '2025-11-10 20:40:00', 103.5, 'OK'),
    ('sensor_3', '2025-11-10 20:35:00', 107.2, 'FAIL'),
    ('sensor_1', '2025-11-10 20:30:00', 101.7, 'OK')
]

cols = ["id", "timestamp", "pressure", "status"]

raw_df = (
    spark
    .createDataFrame(sensor_data, cols)
)
raw_df.printSchema()

print("=========Source Schema==============")
actual_schema = raw_df.schema.fields
print(actual_schema)

print("===========method1 to exract columns as list=======")
source_schema_list1 = raw_df.schema.fieldNames()
print(source_schema_list1)

print("============method2 to extract column as list======")
source_schema_list2 = [
    fld.name for fld in raw_df.schema.fields
]
print(source_schema_list2)

print("========Source Schema converted as dict=====")
source_schema_dict = {
    fld.name : fld.dataType.simpleString()
    for fld in actual_schema
}
print(source_schema_dict)
print("=======Operation on dict=============")
print(source_schema_dict.keys())
print(source_schema_dict.values())
print(source_schema_dict.items())
source_new_dict = { }
for key_1 in source_schema_dict:
    print(key_1, source_schema_dict[key_1])
    source_new_dict[key_1] = source_schema_dict[key_1]
print(source_new_dict)

expected_schema = StructType([
    StructField("id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("pressure", StringType(), True),
    StructField("Region", StringType(), True)
])
print("==========Expected Schema==============")
print(expected_schema)

print("=======Expected Schema Converting to dict=========")
exp_schema_dict ={
    field_extract.name : field_extract.dataType.simpleString()
    for field_extract in expected_schema
}
print(exp_schema_dict)

print("=========Coverting expected schema as list========")
exp_schema_list1 = [ field_col.name for field_col in expected_schema]
print(exp_schema_list1)


source_change = []
source_missing_cols = []
for col1 in source_schema_list2:
    if col1 not in exp_schema_list1:
        source_change.append(col1)
for col2 in exp_schema_list1:
    if col2 not in source_schema_list2:
        source_missing_cols.append(col2)
    else:
        print(f"{col2} validation successful")

if len(source_change) >=1 :
    print("Schema changes below")
    print(source_change)
else:
    print("There is no schema changes")

print(source_missing_cols)

print("===========converting schema as dict with json===")
import json
exp_json_method_dict = json.loads(expected_schema.json())
print(exp_json_method_dict)