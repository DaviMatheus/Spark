

from pyspark.sql import SparkSession


spark = SparkSession \
    .builder \
    .appName("Topics") \
    .getOrCreate()

stats = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", 'localhost:9090') \
    .option("write", "topics") \
    .load()

q = stats \
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .writeStream \
    .format('console') \
    .outputMode('append') \
    .trigger(processingTime='3 seconds')\
    .start() 

q.awaitTermination()