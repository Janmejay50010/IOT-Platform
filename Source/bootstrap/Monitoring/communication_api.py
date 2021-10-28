from kafka import KafkaConsumer
import time
import json

kafkaIPPort = '52.15.89.83:9092'

def monitor():
    consumer = KafkaConsumer('monitor',
    bootstrap_servers=[kafkaIPPort],
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    consumer_timeout_ms=10000*6*10
    )
    for message in consumer:
        yield message.value
