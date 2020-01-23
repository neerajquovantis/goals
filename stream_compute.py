# ~/spark-2.3.3-bin-hadoop2.7/bin/spark-submit stream_compute.py localhost 9999
# bin/kafka-console-producer.sh --topic my-stream --broker-list 127.0.0.1:9092
# "datetime": "2019-12-30 19:04:45", "order_id": "0a1db7cb-9fda-4f93-8425-6edae003cd29", "store_id": 1, "store_name": "dominos_GK", "items": {"1_non veg taco mexicana_2": 198, "2_non veg margherita medium_1": 238}, "order_total": 436}
# new branch goals-100
# ~/spark-2.4.4-bin-hadoop2.7/bin/spark-submit --conf "spark.executor.extraJavaOptions=-verbose:class"  --conf "spark.driver.extraJavaOptions=-verbose:class" --jars /home/neeraj/extlib/spark-sql-kafka-0-10_2.11-2.4.4.jar,/home/neeraj/extlib/kafka-clients-0.10.0.0.jar stream_compute.py | grep "Batch:" -A 10

import json
import ast

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import from_json
from pyspark.sql.functions import col
from pyspark.streaming.kafka import KafkaUtils
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from pyspark.sql.types import StructType , StringType , LongType , IntegerType, DateType, TimestampType, MapType


schema = StructType().add("datetime", TimestampType()).add("order_id", StringType()).add("store_id",IntegerType()).add("store_name",StringType()).add("items", MapType(StringType(), IntegerType())).add("order_total",IntegerType())

spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()


dsraw = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
    .option("subscribe", "my-stream") \
    .load().select(from_json(col("value").cast("string"), schema).alias("parsed_values")) \
    .select(col("parsed_values.*"))

def process_row(row):
    print{}
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect('goals')
    session.row_factory = dict_factory
    _query = "INSERT INTO orders (datetime,order_id,store_id,items,order_total,store_name) values ('{datetime}',{order_id},{store_id},{items},{order_total},'{store_name}')".format(datetime=row["datetime"], order_id=row["order_id"], store_id=row["store_id"], 
        items=ast.literal_eval(json.dumps(row["items"])), order_total=row["order_total"], store_name=row["store_name"])
    if (row["datetime"]):
        rows = session.execute(_query)

pre_query = dsraw.writeStream.foreach(process_row).start()

## Start running the query that prints the output
query = dsraw\
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .queryName("t10") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()

