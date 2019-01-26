from kafka import KafkaProducer
import json
import random
import time
import datetime
import os

kafka_servers = os.getenv('KAFKA_SERVERS', "127.0.0.1:9092")
producer = KafkaProducer(bootstrap_servers=kafka_servers)
producer.flush()

stations = []

for i in range(5):
    station = {
        "id": "ST%d" % (1000 + i),
        "location": {
            "longtitude": random.randint(34.243563 * 1000000, 34.306175 * 1000000) / 1000000.0,
            "latitude": random.randint(108.896280 * 1000000, 108.994882 * 1000000) / 1000000.0
        }
    }
    stations.append(station)

while True:
    for i in range(5):
        station = stations[i]
        station["timestamp"] = str(datetime.datetime.now())

    producer.send('station_information', json.dumps(stations).encode('utf-8'))
    producer.flush()

    time.sleep(15)
