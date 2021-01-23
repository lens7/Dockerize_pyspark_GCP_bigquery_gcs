import google.cloud
import pyspark as ps
import google.cloud.bigquery
from pyspark.sql import SparkSession
from pyspark import SparkContext

#spark = SparkSession.builder.appName('spark-bigquery-demo').getOrCreate()
spark = SparkSession.builder.appName('spark-gcs-demo').config("spark.jars", "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop2-latest.jar").getOrCreate()


#notesnew = spark.read.option("header", "false").csv("gs://mimic-db/notesnew.csv")
notesnew = spark.read.option("header", "True").csv("gs://test-245/air.csv", header=True, inferSchema=True)
notesnew.show()
print("\n Done Reading data in spark from a file in bucket ")
notesnew.write.option("header",True).csv("gs://test-245/air_quality")
print("\n Done writing to gcs bucket")
