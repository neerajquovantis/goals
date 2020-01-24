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
import uuid



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

def save_orders(row):
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect('goals')
    session.row_factory = dict_factory
    _query = "INSERT INTO orders (datetime,order_id,store_id,items,order_total,store_name) values (%s,%s,%s,%s,%s,%s)"
    print(type(row["datetime"]), type)
    val = [row["datetime"],uuid.UUID(row["order_id"]),row["store_id"],row["items"],row["order_total"],row["store_name"]]
    if (row["datetime"]):
        rows = session.execute(_query,val)

        print("get items start :::")
        print(row)
        if (row["datetime"]):
            store_id = row["store_id"]
            store_name = row["store_name"]
            for k, v in ast.literal_eval(json.dumps(row["items"])).items():
                print(k , v)
                item_id_name_quantity = k.split("_")
                item_id = item_id_name_quantity[0]
                item_name = item_id_name_quantity[1]
                item_quantity = item_id_name_quantity[2]
                item_total_price = v

                print(item_id, item_name, item_quantity)
                _query_menu = "select goods_required, item_price from menu where item_id={item_id} allow filtering;".format(item_id=item_id)
                rows_menu = session.execute(_query_menu)
                for item in rows_menu:
                    print("menu item")
                    print(item["goods_required"])
                    print(item["item_price"])
                    item_price = item["item_price"]

                _query_store = "select goods from stores where store_id={store_id};".format(store_id=store_id)
                rows_store = session.execute(_query_store)
                for item in rows_store:
                    print("store goods")
                    print(item["goods"])

                data_schema = [StructField('item_id', IntegerType(), True), StructField('item_name', StringType(), True), StructField('item_id', IntegerType(), True), StructField('item_name', StringType(), True), StructField('item_quantity', IntegerType(), True), StructField('item_price', IntegerType(), True), StructField('item_total_price', IntegerType(), True)]
                # finaldf = spark.createDataFrame(
                #     [(store_id, store_name, item_id, item_name, item_quantity, item_price, item_total_price),],
                #     ['Store_Id', 'Store_Name', 'Item_Id','Item_Name', 'Item_Quantity', 'Item_Price', 'Item_Total_Price']
                # )
                # finaldf.show(truncate=false)


            print("rows by get_items")

# pre_query1 = dsraw.writeStream.foreach(save_orders).start()

## Start running the query that prints the output
query = dsraw \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .queryName("t10") \
    .option("truncate", "false") \
    .start()

# dfnew.writeStream
#     .format("orc")        // can be "orc", "json", "csv", etc.
#     .option("path", "/orders/flowperitem/")   
#     .start()


ds_processing = dsraw.select(col('order_id'), explode(col('items')).alias("item", "amount"), split("item","_").getItem(0).alias("item_id"), split("item","_").getItem(1).alias("item_name"),
    split("item","_").getItem(2).alias("item_quantity"))


pre_query2 = ds_processing \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .queryName("ds_processing") \
    .option("truncate", "false") \
    .start()


query.awaitTermination()

