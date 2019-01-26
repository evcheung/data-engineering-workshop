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

while True:
    for i in range(5):
        bikes_count = random.randint(0, 50)
        status = {
            "id": "ST%d" % (1000 + i),
            "bikes_count": bikes_count,
            "timestamp": str(datetime.datetime.now())
        }
        stations.append(status)

    producer.send('station_status', json.dumps(stations).encode('utf-8'))
    producer.flush()
    stations.clear()

    time.sleep(5)
