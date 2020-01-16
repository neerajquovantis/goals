# ~/spark-2.3.3-bin-hadoop2.7/bin/spark-submit stream_compute.py localhost 9999
# bin/kafka-console-producer.sh --topic my-stream --broker-list 127.0.0.1:9092
# nc -lk 9999
# new branch goals-100
# ~/spark-2.3.3-bin-hadoop2.7/bin/spark-submit --conf "spark.executor.extraJavaOptions=-verbose:class"  --conf "spark.driver.extraJavaOptions=-verbose:class" --jars /extlib/spark-sql-kafka-0-10_2.11-2.3.3.jar,/extlib/kafka-clients-0.8.2.1.jar,~/extlib/spark-sql-kafka-0-10_2.11-2.3.3.jar,~/extlib/kafka-clients-0.8.2.1.jar stream_compute.py | grep "Batch:" -A 10

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import from_json
from pyspark.sql.functions import col
from pyspark.streaming.kafka import KafkaUtils
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from pyspark.sql.types import StructType , StringType , LongType , IntegerType, DateType, TimestampType, MapType


#schema = StructType().add("datetime", DateType()).add("order_id", StringType()).add("store_id",IntegerType()).add("store_name",StringType()).add("items",MapType(StringType, IntegerType)).add("order_total",IntegerType())

schema = StructType().add("datetime", TimestampType()).add("order_id", StringType()).add("store_id",IntegerType()).add("store_name",StringType())

spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()


dsraw = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
    .option("subscribe", "my-stream") \
    .option("startingOffsets", "earliest") \
    .load().select(from_json(col("value").cast("string"), schema).alias("parsed_values")) \
    .select(col("parsed_values.*"))


cluster = Cluster(['127.0.0.1'])
session = cluster.connect('goals')
session.row_factory = dict_factory
rows = session.execute("SELECT * FROM orders LIMIT 10")

print("rowssssss : "+ str(rows[0]['items']))


## Start running the query that prints the output

query = dsraw\
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .queryName("t10") \
    .start()

query.awaitTermination()
