from kafka import KafkaProducer
import json
import random
import time
import datetime
import os

def generate_orders(count):
    orders = []
    for i in range(count):
        createdTime = datetime.datetime.now() + datetime.timedelta(seconds=i)
        order = {
            "orderId": "%s%05d" % (createdTime.strftime("%Y%m%d%H%M"), i % (count / 3 + 1)),
            "itemId": "%05d" % (i + 1),
            "quantity": i *  count / 2  + 1,
            "price": 100+i,
            "timestamp": createdTime.isoformat()
        }
        orders.append(order)
    return orders

if __name__ == "__main__":
    kafka_servers = os.getenv('KAFKA_SERVERS', "127.0.0.1:9092")

    producer = KafkaProducer(bootstrap_servers=kafka_servers)
    producer.flush()

    while True:
        orders = generate_orders(10)
        time.sleep(12)
        producer.send('orders', json.dumps(orders).encode('utf-8'))
