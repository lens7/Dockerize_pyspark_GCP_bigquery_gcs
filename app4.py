import google.cloud
import pyspark as ps
import google.cloud.bigquery
from pyspark.sql import SparkSession
from pyspark import SparkContext

#spark = SparkSession.builder.appName('spark-bigquery-demo').getOrCreate() #this is normal session creator
spark = SparkSession.builder.appName('spark-gcs-bq-demo').config("spark.jars", "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop2-latest.jar,http://storage.googleapis.com/spark-lib/bigquery/spark-bigquery-latest_2.12.jar").getOrCreate()
#the above one has got the connectors and configuration established


bucket = "temporary_bucket_name" # used by bigquery read/write operations as temporary middle place 
spark.conf.set('temporaryGcsBucket', bucket)

words = spark.read.format('bigquery').option('table', 'project_name:dataset_name.table_name').load() #change the bigquery id here
words.createOrReplaceTempView('words')

word_count = spark.sql('SELECT * FROM words limit 10')

# Saving the data to BigQuery
#copyfornotesoriginalTWO.write.mode('append').format('bigquery').option('table', 'test.xyz').save()

word_count.show()

notesnew = spark.read.option("header", "false").csv("gs://bucket_name/folder_name/note.csv")
notesnew.show()
print("\n Read data in spark from a file in bucket\n")

notesnew.write.option("header",True).csv("gs://blue-prints-as-code/siddharth-backup/")
print("\n Done writing")
#notesnew.write.format("csv").save("gs://blue-prints-as-code/siddharth-backup/mytable")
#notesnew.write.format("csv").option("path", "gs://blue-prints-as-code/siddharth-backup/my_table").save()
#notesnew.coalesce(1).write.format('csv').save('gs://blue-prints-as-code/siddharth-backup/my_table/my.csv',header = 'true')
