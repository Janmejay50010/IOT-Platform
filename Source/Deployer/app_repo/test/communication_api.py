import producer_api as papi
import json
import threading
	
kafkaIPPort = '52.15.89.83:9092'

def depmanager_senmanager(data):
    papi.send_message('dep_sen',data)

def senmanager_depmanager():
    from kafka import KafkaConsumer
    consumer = KafkaConsumer('dep_sen',
    bootstrap_servers=[kafkaIPPort],
    enable_auto_commit=True,
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    for message in consumer:
        yield message.value

def depmanager_senmanager_rep(data):
    papi.send_message('dep_sen_rep',data)

def senmanager_depmanager_rep():
    from kafka import KafkaConsumer
    consumer = KafkaConsumer('dep_sen_rep',
    bootstrap_servers=[kafkaIPPort],
    enable_auto_commit=True,
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    for message in consumer:
        yield message.value 

def get_sensor_data(topi):
    from kafka import KafkaConsumer
    consumer = KafkaConsumer(topi,
    bootstrap_servers=[kafkaIPPort],
    enable_auto_commit=True,
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    for message in consumer:
        yield message.value 
