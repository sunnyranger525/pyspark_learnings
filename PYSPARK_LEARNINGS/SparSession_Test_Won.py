import os
import sys
python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path
os.environ['HADOOP_HOME'] =r"C:\hadoop"
os.environ['JAVA_HOME'] = r'C:\Program Files\Eclipse Adoptium\jdk-17.0.16.8-hotspot'

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()

data = [
    (1,)
]
data_cols = ["id"]

df = spark.createDataFrame(data, data_cols)
df.show()