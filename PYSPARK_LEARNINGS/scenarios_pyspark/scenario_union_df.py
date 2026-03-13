import os
import sys

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path
os.environ['HADOOP_HOME'] =r"C:\hadoop"
os.environ['JAVA_HOME'] = r'C:\Program Files\Eclipse Adoptium\jdk-17.0.16.8-hotspot'

##########################Above code is Configurations ##############################
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Union_check").master("local[*]").getOrCreate()

from pyspark.sql.functions import *
data = [
    ("A", "Jan", 100),
    ("A", "Jan", 500),
    ("A", "Feb", 200),
    ("A", "Jan", 100),
    ("B", "Jan", 300),
    ("B", "Feb", 150),
    ("B", "Mar", 400)
]

data_cols = ["user_id", "month", "sales"]

# create dataframe
from pyspark.sql import functions as f
df_raw = spark.createDataFrame(data, data_cols)
df = df_raw.withColumn("amount", f.col("sales"))
# print("Number of Paritions", df.rdd.getNumPartitions())
df = df.repartition(4, "user_id")
df = df.withColumn("partitionId", f.spark_partition_id())
# print("Number of Paritions ", df.rdd.getNumPartitions())
df.show()
# df.explain()
# df_fine = (
#     df
#     .groupBy("user_id").pivot("month")
#     .agg(
#         f.sum("sales").alias("sale"),
#         f.sum("amount").alias("amount")
#     )
#     .withColumn("partition_id", f.spark_partition_id())
# )
# df_fine.explain(True)
value = input("Enter some text to exist .............")
spark.stop()


