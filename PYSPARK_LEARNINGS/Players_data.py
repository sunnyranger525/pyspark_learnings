import os
import urllib.request
import ssl
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys
python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path
os.environ['HADOOP_HOME'] =r"C:\hadoop"
#os.environ["PATH"] += ";C:\\hadoop\\bin"
os.environ['JAVA_HOME'] = r'C:\Program Files\Eclipse Adoptium\jdk-17.0.16.8-hotspot'
######################🔴🔴🔴################################
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 pyspark-shell'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-avro_2.12:3.5.4 pyspark-shell'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4 pyspark-shell'

conf = SparkConf().setAppName("pyspark") \
    .setMaster("local[*]") \
    .set("spark.driver.host","localhost") \
    .set("spark.default.parallelism", "1")

sc = SparkContext(conf=conf)

spark = SparkSession.builder.getOrCreate()

#spark.read.format("csv").load("data/test.txt") \
    #.toDF("Success").show(20, False)

##################🔴🔴🔴🔴🔴🔴 -> DON'T TOUCH ABOVE CODE -- TYPE BELOW ####################################

print("======== Creating medals data frame=======")
Game = [
    (2017, 2, 1, 1, 2),
    (2018, 3, 1, 3, 2),
    (2019, 3, 1, 1, 3)
]

cols = [
    'year',
    'Wimbledon',
    'Fr_open',
    'US_open',
    'Au_open'
]

Games_medals = (
    spark
    .createDataFrame(Game, cols)
)
#Games_medals.show()

print("=======Players DATA and Creating DATA Frame======")
playersData = [
    (1, 'Nadal'),
    (2, 'Federer'),
    (3, 'Novak')
]

cols_player = ["id", "Name"]

players = spark.createDataFrame(playersData, cols_player)
# players.show()

df1 = Games_medals.select(
    col("year"),
    col("Wimbledon")
)
df2 = Games_medals.select(
    col("year"),
    col("Fr_open")
)
df3 = Games_medals.select(
    col("year"),
    col("US_open")
)
df4 = Games_medals.select(
    col("year"),
    col("Au_open")
)

stage_df = (
    df1
    .union(df2)
    .union(df3)
    .union(df4)
)
stage_df.show()

# Cont player id to find how many times each player won

won_count_df = (
    stage_df.groupby(col("Wimbledon").alias("Play_id"))
    .agg(count("*").alias("won_cnt"))
)
won_count_df.show()

# add player data and transform data

player_join_tr_data = (
    players
    .join(
        won_count_df,
        players["id"] == won_count_df["Play_id"],
        "inner"
    )
    .select(
        col("id"),
        col("Name"),
        col("won_cnt")
    )
)

player_join_tr_data \
    .write.format("parquet") \
    .mode("overwrite") \
    .option("path", r"E://DATA/parquetfiles/output/players/") \
    .save()
print("Data has been saved at target....")
player_join_tr_data.show()

# import time
# time.sleep(600)
# input("enter something to exit ....:>>: ")
# spark.stop()