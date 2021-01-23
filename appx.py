import google.cloud
import pandas as pd
import pyspark as ps
import google.cloud.bigquery
from pyspark.sql import SparkSession
from pyspark import SparkContext

#spark = SparkSession.builder.appName('spark-bigquery-demo').getOrCreate()
spark = SparkSession.builder.appName('spark-gcs-demo').config("spark.jars", "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop2-latest.jar").getOrCreate()

nota = pd.read_csv("gs://fulladdress")
notesnew = spark.createDataFrame(nota)
#notesnew = spark.read.option("header", "false").csv("gs://xyz/notes.csv")
notesnew.show()
print("\n Done Reading data in spark from a file in bucket ")
notesnew.write.option("header",True).csv("gs://bucket/folder")
print("\n Done writing to gcs bucket")
