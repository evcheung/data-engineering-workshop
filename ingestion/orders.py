from kafka import KafkaProducer
import json
import random
import time
import datetime
import os

kafka_servers = os.getenv('KAFKA_SERVERS', "127.0.0.1:9092")

producer = KafkaProducer(bootstrap_servers=kafka_servers)
producer.flush()

orders = []

while True:
    for i in range(10):
        createdTime = datetime.datetime.now() + datetime.timedelta(seconds=i)
        order = {
            "orderId": "%s%d" % (createdTime.strftime("%Y%m%d%H%M%S"), 100+i),
            "productId": "ST%d" % (1000 + i),
            "quantity": i,
            "price": 100+i,
            "timestamp": str(createdTime)
        }
        orders.append(order)

    time.sleep(7)
    producer.send('orders', json.dumps(orders).encode('utf-8'))