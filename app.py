import google.cloud
import pyspark as ps
import google.cloud.bigquery
from pyspark.sql import SparkSession
from pyspark import SparkContext

#spark = SparkSession.builder.appName('spark-bigquery-demo').getOrCreate()
spark = SparkSession.builder.appName('spark-gcs-demo').config("spark.jars", "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop2-latest.jar").getOrCreate()


notesnew = spark.read.option("header", "false").csv("gs://bucketname/foldername/filename") #full address your file is needed here
notesnew.show() # prints the dataframe
print("\n Done Reading data in spark from a file in bucket ")
notesnew.write.option("header",True).csv("gs://destination_bucket/destination_folder") #the file will be saved as part files being hadoop based system
print("\n Done writing to gcs bucket")
