from kafka import KafkaProducer
import json
import random
import time
import datetime

producer = KafkaProducer(bootstrap_servers='kafka:9092')
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
