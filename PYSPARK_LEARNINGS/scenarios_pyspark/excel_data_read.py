import os
import sys
from pyspark.sql.functions import spark_partition_id

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path
os.environ['HADOOP_HOME'] =r"C:\hadoop"
os.environ['JAVA_HOME'] = r'C:\Program Files\Eclipse Adoptium\jdk-17.0.16.8-hotspot'

##########################Above code is Configurations ##############################
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Excel_read").master("local[*]")\
    .config("spark.jars.packages", "com.crealytics:spark-excel_2.12:0.13.7")\
    .getOrCreate()

from pyspark.sql import functions as f

source_path = r"E:/DATA/excelfiles/testfile_02.xlsx"
target = r"E:/DATA/parquetfiles/excel/testfile_02/"

excel_df = spark.read.format("com.crealytics.spark.excel")\
    .option("header", "true")\
    .option("dataAddress", "'emp_data!A1")\
    .option("inferSchema", "true")\
    .load(source_path)
excel_df.show()
