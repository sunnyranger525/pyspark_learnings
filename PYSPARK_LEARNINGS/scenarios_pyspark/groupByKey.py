import os
import sys
from pyspark.sql.functions import spark_partition_id

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path
os.environ['HADOOP_HOME'] =r"C:\hadoop"
os.environ['JAVA_HOME'] = r'C:\Program Files\Eclipse Adoptium\jdk-17.0.16.8-hotspot'

##########################Above code is Configurations ##############################
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("groupByKey").master("local[*]").getOrCreate()
sc = spark.sparkContext

rdd = sc.parallelize([
    ("s1", 10),
    ("s1", 20),
    ("s2", 5)
])

reduce_rdd = rdd.reduceByKey(lambda x, y : x+y)
print(reduce_rdd.collect())

group_rdd = rdd.groupByKey().mapValues(sum)
print()



