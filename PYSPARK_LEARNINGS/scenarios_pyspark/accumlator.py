import os
import sys
from pyspark.sql.functions import spark_partition_id

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path
os.environ['HADOOP_HOME'] =r"C:\hadoop"
os.environ['JAVA_HOME'] = r'C:\Program Files\Eclipse Adoptium\jdk-17.0.16.8-hotspot'

##########################Above code is Configurations ##############################
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("accumlator").master("local[*]").getOrCreate()

data = [1,2,3,4,5,6,7]

sc = spark.sparkContext
acc = sc.accumulator(0)
rdd = sc.parallelize(data)

def process(x):
    if x % 2 == 0:
        acc.add(1)
    return x*10
rdd.map(process).collect()
print(acc)

data2 = [
    (1, 25.0, 100.5),
    (2, None, 98.2),
    (3, 30.1, None),
    (4, None, None)
]

data_cols = ["id", "temp", "pressure"]

data_df = spark.createDataFrame(data2, data_cols)
data_df.show()

temp_acc = sc.accumulator(0)
press_acc = sc.accumulator(0)

def count_null(row):
    if row["temp"] is None:
        temp_acc.add(1)
    if row["pressure"] is None:
        press_acc.add(1)
print(data_df.rdd.collect()[0])
data_df.rdd.foreach(count_null)
print(temp_acc)
print(press_acc)










