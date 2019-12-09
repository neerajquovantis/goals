# ~/spark-2.3.3-bin-hadoop2.7/bin/spark-submit stream_compute.py localhost 9999
# bin/kafka-console-producer.sh --topic my-stream --broker-list 127.0.0.1:9092
# nc -lk 9999
# new branch goals-100

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.streaming.kafka import KafkaUtils

spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .config("spark.jars", "/home/neeraj/Downloads/_jar/kafka-clients-0.10.0.2.jar,/home/neeraj/Downloads/_jar/spark-sql-kafka-0-10_2.11-2.3.3.jar") \
    .getOrCreate()


dsraw = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
    .option("subscribe", "my-stream") \
    .option("startingOffsets", "earliest") \
    .load()

ds = dsraw.selectExpr("CAST(value AS STRING)")

query = dsraw.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()    

# val df = dsraw.select(explode(split($"value".cast("string"), "\\s+")).as("word"))
#   .groupBy($"word")
#   .count


# display(df.select($"word", $"count"))
# dsraw.printSchema()

# dsraw.show()
# print(dsraw)



# ds.show()

# ds.printSchema()

# rawQuery = dsraw \
#         .writeStream \
#         .queryName("qraw")\
#         .format("memory")\
#         .start()

# alertQuery = ds \
#         .writeStream \
#         .queryName("qalerts")\
#         .format("memory")\
#         .start()

# alerts = spark.sql("select * from qalerts")
# alerts.show()

# # Create DataFrame representing the stream of input lines from connection to localhost:9999
# lines = spark \
#     .readStream \
#     .format("socket") \
#     .option("host", "localhost") \
#     .option("port", 9999) \
#     .load()

# # Split the lines into words
# words = ds.select(
#    explode(
#        split(ds.value, " ")
#    ).alias("word")
# )

# # # # Generate running word count
# wordCounts = words.groupBy("word").count()

# #  # Start running the query that prints the running counts to the console
# query = wordCounts \
#     .writeStream \
#     .outputMode("complete") \
#     .format("console") \
#     .start()

query.awaitTermination()
