# ~/spark-2.3.3-bin-hadoop2.7/bin/spark-submit stream_compute.py localhost 9999
# bin/kafka-console-producer.sh --topic my-stream --broker-list 127.0.0.1:9092
# "datetime": "2019-12-30 19:04:45", "order_id": "0a1db7cb-9fda-4f93-8425-6edae003cd29", "store_id": 1, "store_name": "dominos_GK", "items": {"1_non veg taco mexicana_2": 198, "2_non veg margherita medium_1": 238}, "order_total": 436}
# new branch goals-100
# ~/spark-2.4.4-bin-hadoop2.7/bin/spark-submit --conf "spark.executor.extraJavaOptions=-verbose:class"  --conf "spark.driver.extraJavaOptions=-verbose:class" --jars /home/neeraj/extlib/spark-sql-kafka-0-10_2.11-2.4.4.jar,/home/neeraj/extlib/kafka-clients-0.10.0.0.jar stream_compute.py | grep "Batch:" -A 10

import ast
import json
import uuid
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from pyspark.sql import functions as F, SparkSession
from pyspark.sql.functions import col, explode, from_json, split, window
from pyspark.sql.types import IntegerType, MapType, StringType, StructType, TimestampType

schema = StructType().add("datetime", TimestampType()).add("order_id", StringType()).add("store_id",IntegerType()).add("store_name",StringType()).add("items", MapType(StringType(), IntegerType())).add("order_total",IntegerType())

spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()

# todo: check if we can avoid specifying the schema in spark2.4
dsraw = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
    .option("subscribe", "my-stream") \
    .load().select(from_json(col("value").cast("string"), schema).alias("parsed_values")) \
    .select(col("parsed_values.*"))

class SaveOrders:
    def open(self, partition_id, epoch_id):
        try:
            self.cluster = Cluster(['127.0.0.1'])
            self.session = self.cluster.connect('goals')
            self.session.row_factory = dict_factory
            return True
        except Exception as e:
            print(e)
            return False

    def process(self, row):
        _query = "INSERT INTO orders (datetime,order_id,store_id,items,order_total,store_name) values (%s,%s,%s,%s,%s,%s)"
        print(type(row["datetime"]), type)
        val = [row["datetime"],uuid.UUID(row["order_id"]),row["store_id"],row["items"],row["order_total"],row["store_name"]]
        if (row["datetime"]):
            rows = self.session.execute(_query,val)

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
                    rows_menu = self.session.execute(_query_menu)
                    for item in rows_menu:
                        print("menu item")
                        print(item["goods_required"])
                        print(item["item_price"])
                        item_price = item["item_price"]

                    _query_store = "select goods from stores where store_id={store_id};".format(store_id=store_id)
                    rows_store = self.session.execute(_query_store)
                    for item in rows_store:
                        print("store goods")
                        print(item["goods"])
                print("rows by get_items")

    def close(self, error):
        pass

def prepare_write_to_hdfs(df):
    ds_processing = df.select(col('datetime'), col('order_id'), col('store_id'), col('store_name'), explode(col('items')).alias("item", "amount"), split("item","_").getItem(0).alias("item_id"), split("item","_").getItem(1).alias("item_name"),
        split("item","_").getItem(2).cast(IntegerType()).alias("item_quantity"))

    ds_processing_1 = ds_processing.withWatermark("datetime", "60 minutes").groupBy(
        window(col("datetime"), "60 minutes", "60 minutes").alias("datetime_range"),
        col("item_name"), col("store_name"), col("store_id")
    ).agg(F.min(col("amount")).alias("min_amount"),F.max(col("amount")).alias("max_amount"),F.avg(col("amount")).alias("avg_amount"),F.sum(col("amount")).alias("amount_sum"),F.sum(col("item_quantity")).alias("item_quantity_sum"))

    ds_processing_hour_generated = ds_processing_1.withColumn("hour_generated", F.from_unixtime(F.unix_timestamp(ds_processing_1.datetime_range.getItem('start')), "yyyyMMddhh"))
    return ds_processing_hour_generated


query_save_order = dsraw.writeStream.foreach(SaveOrders()).start()
query_write_to_hdfs = prepare_write_to_hdfs(dsraw) \
    .writeStream \
    .outputMode("append") \
    .format("orc") \
    .option("path", "/orders/flowperitem/orc") \
    .option("checkpointLocation", "/orders/flowperitem/checkpoint") \
    .partitionBy("hour_generated") \
    .start()

# print("schema_ds_processing")
# print(ds_processing.printSchema())

## Start running the query that prints the output
# query = dsraw \
#     .writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .queryName("t10") \
#     .option("truncate", "false") \
#     .start()

# pre_query2 = ds_processing_hour_generated \
#     .writeStream \
#     .outputMode("complete") \
#     .format("console") \
#     .queryName("ds_processing") \
#     .option("truncate", "false") \
#     .start()


query_write_to_hdfs.awaitTermination()

