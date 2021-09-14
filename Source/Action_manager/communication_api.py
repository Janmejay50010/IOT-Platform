import prodapi
import json
import threading,datetime
import ast
# import monitoring_db
#Monitoring comm_functions start here
kafkaIPPort = '52.15.89.83:9092'

def depmanager_actmanager(data):
    prodapi.send_message('dep_act',data)

def actmanager_depmanager(func):
    from kafka import KafkaConsumer
    consumer = KafkaConsumer('dep_act',
    bootstrap_servers=[kafkaIPPort],
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    for message in consumer:
        print("Hello") 
        print(message.value)
        th = threading.Thread(target=func,args=(message.value,))
        th.start()

