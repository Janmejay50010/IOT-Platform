from kafka import KafkaProducer
import json

kafkaIPPort = '52.15.89.83:9092'

producer = KafkaProducer(bootstrap_servers=kafkaIPPort,
value_serializer=lambda v: json.dumps(v).encode('utf-8'))        
def send_message(topic,message):
    producer.send(topic, value = message)
    producer.flush()
    