# Spark

## 1. Apache Spark com Socket

[INSTALÇÃO SPARK](https://spark.apache.org/streaming/)
[AJUDA TUTORIAL](https://pt.linux-console.net/?p=2658)

#### 1.2 EXECUÇÃO
```
$ nc -lp 9999       *Inicia o NetCat       
$ cd $SPARK_HOME                               
$ bin/spark-submit $PATH/via_socket.py  # abre de acordo com o seu Path
                                               
```

### 2 Spark com Kafka

[INSTALÇÃO Kafta](https://kafka.apache.org/downloads)
[AJUDA TUTORIAL](https://kafka.apache.org/quickstart)

```
$ tar -xzf kafka_2.13-3.2.1.tgz
$ cd kafka_2.13-3.2.1
$ bin/zookeeper-server-start.sh config/zookeeper.properties
EM OUTRO TERMINAL
$ bin/kafka-server-start.sh config/server.properties
```
### 2.1 Crie os topicos
```
EXEMPLO:
$ $ bin/kafka-topics.sh --create --topic quickstart-events --bootstrap-server localhost:9092

$ cd $KAFKA_HOME
NO NOSSO CASO
# Criando o topico  no server 'localhost:9092'
$ bin/kafka-topics.sh --create --topic topics --bootstrap-server localhost:9092
```

### 2.2 EXECUTAR SPARK
```
$ cd $SPARK_HOME
CONTADOR
$ bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 $path/via_kafka.py
PARA VISUALIZAR
$ bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 $PROJECT_HOME/complemento.py
$ bin/kafka-console-producer.sh --topic contador_palavras --bootstrap-server localhost:9092

```



```
