import google.cloud
import pyspark as ps
import google.cloud.bigquery
from pyspark.sql import SparkSession
from pyspark import SparkContext

#spark = SparkSession.builder.appName('spark-bigquery-demo').getOrCreate()
spark = SparkSession.builder.appName('spark-gcs-bq-demo').config("spark.jars", "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop2-latest.jar,http://storage.googleapis.com/spark-lib/bigquery/spark-bigquery-latest_2.12.jar").getOrCreate()
#sc = SparkContext('local', 'test')


bucket = "test-245"
spark.conf.set('temporaryGcsBucket', bucket)

words = spark.read.format('bigquery').option('table', 'coe-foundation-215839:mmic_data.notes').load()
words.createOrReplaceTempView('words')

word_count = spark.sql('SELECT * FROM words limit 10')

# Saving the data to BigQuery
#copyfornotesoriginalTWO.write.mode('append').format('bigquery').option('table', 'test.xyz').save()

word_count.show()

#notesnew = spark.read.option("header", "false").csv("gs://mimic-db/notesnew.csv")
notesnew = spark.read.option("header", "True").csv("gs://test-245/air.csv", header=True, inferSchema=True)
notesnew.show()
print("\n Read data in spark from a file in bucket and adding desired columns names \n")

#notesnew.write.option("header",True).csv("gs://blue-prints-as-code/siddharth-backup/")
#notesnew.write.format("csv").save("gs://blue-prints-as-code/siddharth-backup/mytable")
#notesnew.write.format("csv").option("path", "gs://blue-prints-as-code/siddharth-backup/my_table").save()
#notesnew.coalesce(1).write.format('csv').save('gs://blue-prints-as-code/siddharth-backup/my_table/my.csv',header = 'true')

'''
gcs_bucket = 'blue-prints-as-code'

gcs_filepath = 'gs://{}/my_table/hourly_bids.csv'.format(gcs_bucket)

notesnew.coalesce(1).write \
  .mode('overwrite') \
  .csv(gcs_filepath)
'''
