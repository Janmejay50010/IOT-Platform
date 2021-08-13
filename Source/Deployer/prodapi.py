from kafka import KafkaProducer
import json
import datetime
from time import sleep

kafkaIPPort = '52.15.89.83:9092'

producer = KafkaProducer(bootstrap_servers=kafkaIPPort,
value_serializer=lambda v: json.dumps(v).encode('utf-8'))        
def send_message(topic,message):
    producer.send(topic, value = message)
    producer.flush()
def heart_beat(topic,module_name):
    while True:
        currtime = str(datetime.datetime.now())
        print(topic,module_name,currtime)
        message = currtime+'_{} is Running'.format(module_name)
        print(message)
        producer.send(topic,message)
        producer.flush()
        # producer.send('Deployer_to_Monitor_topic',{'Deployer':'Running'})
        sleep(5)