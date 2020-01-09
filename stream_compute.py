# ~/spark-2.3.3-bin-hadoop2.7/bin/spark-submit stream_compute.py localhost 9999
# bin/kafka-console-producer.sh --topic my-stream --broker-list 127.0.0.1:9092
# nc -lk 9999
# new branch goals-100
# ~/spark-2.3.3-bin-hadoop2.7/bin/spark-submit --conf "spark.executor.extraJavaOptions=-verbose:class"  --conf "spark.driver.extraJavaOptions=-verbose:class" --jars /extlib/spark-sql-kafka-0-10_2.11-2.3.3.jar,/extlib/kafka-clients-0.8.2.1.jar,~/extlib/spark-sql-kafka-0-10_2.11-2.3.3.jar,~/extlib/kafka-clients-0.8.2.1.jar stream_compute.py


from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.streaming.kafka import KafkaUtils
from cassandra.cluster import Cluster
from cassandra.query import dict_factory

spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()
#.config("spark.jars", "/home/neeraj/kafka_2.11-2.3.0/libs/kafka-clients-2.3.0.jar,/home/neeraj/Downloads/_jar/spark-sql-kafka-0-10_2.11-2.3.3.jar") \

dsraw = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
    .option("subscribe", "my-stream") \
    .option("startingOffsets", "earliest") \
    .load()

ds = dsraw.selectExpr("CAST(value AS STRING)")

#query = ds.writeStream \
#    .outputMode("append") \
#    .format("console") \
#    .option("truncate", "false")\
#    .start()    

cluster = Cluster(['127.0.0.1'])
session = cluster.connect('goals')
session.row_factory = dict_factory
rows = session.execute("SELECT * FROM orders LIMIT 10")

print("rowssssss : "+ str(rows[0]['items']))


# # Split the lines into words
words = ds.select(
    explode(
        split(ds.value, " ")
    ).alias("word")
)

# # # # Generate running word count
wordCounts = words.groupBy("word").count()

## Start running the query that prints the running counts to the console
query = wordCounts \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
