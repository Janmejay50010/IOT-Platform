from kafka import KafkaProducer
import json
import datetime
from time import sleep
import threading

kafkaIPPort = '52.15.89.83:9092'

producer = KafkaProducer(bootstrap_servers = kafkaIPPort,
value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def heart_beat(module_name):
    while True:
        currtime = str(datetime.datetime.utcnow())
        message = {
            'moduleName':module_name,
            'currentTime':currtime
        }
        producer.send('monitor',message)
        sleep(5)

def monitor_thread(module_name):
    t = threading.Thread(target=heart_beat,args=(module_name,))
    t.daemon = True
    t.start()
