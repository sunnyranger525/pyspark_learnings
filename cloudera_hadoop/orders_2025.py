# Pyspark Program orders cleaning data
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import *
from pyspark.sql.window import *

spark = SparkSession.builder.appName("orders_2025").getOrCreate()

orders_df = (
    spark.read.format("csv").option("header", "true")
    .option("inferSchema", "true")
    .load("hdfs:///user/projectdata/landing/orders_2025/*.csv")
)
orders_df = orders_df.withColumn("ingest_ts", f.current_timestamp()) \
    .withColumn("file_name", f.lit("orders_2025.csv"))
# write
target_path = "hdfs:///user/projectdata/processed/orders/"
orders_df.write.format("parquet").mode("append").save(target_path)
print("Successfull Completed.................")
