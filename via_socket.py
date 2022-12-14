from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import lit
from pyspark.sql.functions import col, upper

spark = SparkSession \
            .builder \
            .appName("WEB_SOCKET") \
            .getOrCreate()

#Criar DataFrame representando o fluxo de linhas de entrada da conexão para localhost:9999
lines = spark \
         .readStream \
         .format("socket") \
         .option("host", "localhost") \
         .option("port", 9999) \
         .load()

# Divida as linhas em palavras
words = lines.select(
        explode(
   split(lines.value, " ")
   ).alias("word")
   )

 # Gerar contagem de palavras em execução
wordCounts = words.groupBy("word").count()

def foreach_batch_func(df, _):
    total = df \
        .groupBy() \
        .sum() \
        .select(lit('TOTAL').alias('key'), col('sum(count)').alias('value'))

    df.write.format('console').save()
    total.write.format('console').save()

 # Comece a executar a consulta que imprime as contagens em execução no console
query = wordCounts \
          .writeStream \
          .outputMode("complete") \
          .format("console") \
          .start()

query.awaitTermination()