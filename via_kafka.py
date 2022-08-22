from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import substring
from pyspark.sql.functions import window, upper
from pyspark.sql.functions import length
spark = SparkSession \
            .builder \
            .appName("KAFKA") \
            .getOrCreate()
            
# #Criar DataFrame representando o fluxo de linhas de entrada da conexão para localhost:9092 e escrever os toppicos

lines = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("write", "contador_palavras") \
    .option('includeTimestamp', 'true') \
    .load()

# Divida as linhas em palavras
words = lines.select(
    explode(
        split(lines.value, "\s+")).alias("word"),
        lines.timestamp
    )
words = words.select(upper(words.word).alias('word'), words.timestamp)

# Juntar as palavras
wordCounts = words.groupBy("word").count()

# contar o toal de palvaras lidas
total = words \
    .groupBy() \
    .count() \
    .selectExpr("'TOTAL' as key", "CAST(count AS STRING) as value")

lengths = words \
    .filter(length(words.word).isin([6, 8, 11])) \
    .withWatermark("timestamp", "3 seconds") \
    .groupBy(
        window(words.timestamp, "3 seconds", "3 seconds"),
        length(words.word).alias("key")
    ) \
    .count() \
    .selectExpr("CAST(key AS STRING)", "CAST(count AS STRING) as value")

# Contar  6, 8 and 11
lengths = words \
    .filter(length(words.word).isin([6, 8, 11])) \
    .withWatermark("timestamp", "3 seconds") \
    .groupBy(
        window(words.timestamp, "3 seconds", "3 seconds"),
        length(words.word).alias("key")
    ) \
    .count() \
    .selectExpr("CAST(key AS STRING)", "CAST(count AS STRING) as value")



# Contar as palavras S, P and R
letters = words \
    .filter(upper(substring(words.word, 0, 1)).isin(["S", "P", "R"])) \
    .withWatermark("timestamp", "3 seconds") \
    .groupBy(
        window(words.timestamp, "3 seconds", "3 seconds"),
        upper(substring(words.word, 0, 1)).alias("key"),
    ) \
    .count() \
    .selectExpr("key", "CAST(count AS STRING) as value")
# Sinks
qW = wordCounts \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

qT = total \
    .writeStream \
    .outputMode("complete") \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option('topic', "topics") \
    .option('checkpointLocation', '/tmp/spark/total-stats') \
    .start()

qLen = lengths \
    .writeStream \
    .outputMode("update") \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option('topic', "topics") \
    .option('checkpointLocation', '/tmp/spark/len-stats') \
    .trigger(processingTime='3 seconds') \
    .start()

qLet = letters \
    .writeStream \
    .outputMode("update") \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option('topic', "topics") \
    .option('checkpointLocation', '/tmp/spark/let-stats') \
    .trigger(processingTime='3 seconds') \
    .start()

qLen.awaitTermination()
qLet.awaitTermination()
qT.awaitTermination()
qW.awaitTermination()