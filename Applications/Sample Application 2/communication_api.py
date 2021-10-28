import producer_api as papi
import json
import threading
import random
import time

kafkaipport='3.142.69.135:9092'

def initiate_com():
    reqt = 'algo_'+str(random.randint(9,99999))
    papi.send_message('dep_sen',{'topic':reqt})
    return reqt+'_req',reqt+'_rep'

def depmanager_senmanager(data):
    papi.send_message('dep_sen',data)

def senmanager_depmanager():
    from kafka import KafkaConsumer
    consumer = KafkaConsumer('dep_sen',
    bootstrap_servers=[kafkaipport],
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    for message in consumer:
        yield message.value

def app_senmanager(data,topic):
    papi.send_message(topic,data)

def senmanager_app(topic):
    from kafka import KafkaConsumer
    consumer = KafkaConsumer(topic,
    bootstrap_servers=[kafkaipport],
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    for message in consumer:
        yield message.value

def depmanager_senmanager_rep(data):
    papi.send_message('dep_sen_rep',data)

def senmanager_depmanager_rep():
    from kafka import KafkaConsumer
    consumer = KafkaConsumer('dep_sen_rep',bootstrap_servers=[kafkaipport],
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    for message in consumer:
        yield message.value 

def get_sensor_data(topi):
    from kafka import KafkaConsumer
    consumer = KafkaConsumer(topi,
    bootstrap_servers=[kafkaipport],
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    for message in consumer:
        yield message.value 


def appmanager_senmanager(data):
    papi.send_message('app_sen',data)

def senmanager_appmanager():
    from kafka import KafkaConsumer
    consumer = KafkaConsumer('app_sen',
    bootstrap_servers=[kafkaipport],
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    for message in consumer:
        yield message.value
        
def senmanager_actmanager():
    from kafka import KafkaConsumer
    consumer = KafkaConsumer('act_sen',
    bootstrap_servers=[kafkaipport],
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    for message in consumer:
        yield message.value
        
