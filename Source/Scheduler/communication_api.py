import prodapi
import json
import threading

kafkaipport = '52.15.89.83:9092'

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

def scheduler_deployer(data):
    print("to deployer",data)
    prodapi.send_message('sch_dep',data)

def deployer_scheduler():
    from kafka import KafkaConsumer
    consumer = KafkaConsumer('sch_dep',
    bootstrap_servers=[kafkaipport],
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    for message in consumer:
        return message.value
        # th = threading.Thread(target=func,args=(message.value,))
        # th.start()

def scheduler_deployer_abort(data):
    prodapi.send_message('AbortContainer',data)

def deployer_scheduler_abort():
    from kafka import KafkaConsumer
    consumer = KafkaConsumer('AbortContainer',
    bootstrap_servers=[kafkaipport],
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    for message in consumer:
        return message.value
