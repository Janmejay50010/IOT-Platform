import prodapi
import json
import threading

kafkaIPPort = '52.15.89.83:9092'


def appmanager_monitor(data):
    prodapi.send_message('app_mo',data)

def monitor_appmanager(func):
    from kafka import KafkaConsumer
    consumer = KafkaConsumer('app_mo',
    bootstrap_servers=[kafkaipport],
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    for message in consumer:
        # th = threading.Thread(target=func,args=(message.value))
        # th.start()
        return message


def appmanager_senmanager(data):
    prodapi.send_message('app_sen',data)

def senmanager_appmanager(func):
    from kafka import KafkaConsumer
    consumer = KafkaConsumer('app_sen',
    bootstrap_servers=[kafkaipport],
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    for message in consumer:
        # th = threading.Thread(target=func,args=(message.value))
        return message.value
        # th.start()

def appmanager_scheduler(data):
    prodapi.send_message('app_sch',data)

def scheduler_appmanager():
    from kafka import KafkaConsumer
    consumer = KafkaConsumer('app_sch',
    bootstrap_servers=[kafkaipport],
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    for message in consumer:
        return message.value
        # th = threading.Thread(target=func,args=(message.value,))
        # th.start()

def application_deployer(data):
    prodapi.send_message('Get_Logs',data)

def deployer_application():
    from kafka import KafkaConsumer
    consumer = KafkaConsumer('Acquire_logs',
    bootstrap_servers=[kafkaipport],
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    for message in consumer:
        return message.value