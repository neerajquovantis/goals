import time
import random
import datetime
import uuid
import json
from kafka import KafkaProducer
from functools import reduce
from random import randint

stores = ['1_dominos_GK', '3_dominos_KB', '4_dominos_CP', '5_dominos_MT', '6_dominos_FC']
items_list = [	{"1_non veg taco mexicana_2": 198, "2_non veg margherita medium_1": 238},
 		{"3_stuffed garlic bread with cheeze_1": 120, "2_non veg margherita medium_2": 476}, 
 		{"1_non veg taco mexicana_1": 99, "2_non veg margherita medium_2": 476}, 
 		{"4_garlic bread_2": 148, "1_non veg taco mexicana_3": 297, "2_non veg margherita medium_1": 238},
 		{"1_non veg taco mexicana_4": 396, "2_non veg margherita medium_3": 714, "4_garlic bread_4": 296}
]

def create_kafka_producer():
	bootstrap_servers = ['127.0.0.1:9092']
	topicName = 'my-stream'
	producer = KafkaProducer(bootstrap_servers = bootstrap_servers)
	producer = KafkaProducer()
	return topicName,producer

	
def send_to_kafka():
	topicName,producer = create_kafka_producer()
	for i in range(1,10000):
		time.sleep(randint(1,10)/10)
		store = random.choice(stores).split('_')
		store_id = int(store[0])
		store_name = '_'.join(store[1:])
		items = random.choice(items_list)
		order_total = reduce((lambda x, y: x + y),items.values()) 

		# import pdb; pdb.set_trace()
		data = {"datetime": str(datetime.datetime.now()).split(".")[0], "order_id": "{}".format(str(uuid.uuid4())), "store_id": store_id, "store_name": store_name, "items": items, "order_total": order_total}
		data = json.dumps(data).encode('utf-8')
		producer.send(topicName,data)
		print("sent {} time".format(i))

def main():
	send_to_kafka()

if __name__ == '__main__':
    main()
