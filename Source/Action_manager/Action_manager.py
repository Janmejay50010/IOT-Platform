from time import sleep
from json import dumps
from kafka import KafkaProducer
from kafka import KafkaConsumer
import datetime
import sys
import action_service
import action_api
import threading
import monitoring_api as ma

kafkaipport = '52.15.89.83:9092'

# sys.path.append('../communication_module')     # Use if comm_api is in d/f folder
import communication_api as ca

producer = KafkaProducer(bootstrap_servers=kafkaipport,
value_serializer=lambda v: dumps(v).encode('utf-8')) 

def control_producer(topic,message):
    producer.send(topic, value = message)
    producer.flush()



print("Action Manager is Running")
ma.monitor_thread('action_manager')
# message = deptoact
def func(message):
    #Action
    action_type = message['action_type']
    if action_type=='email':
        receiver = message['receiver_id']
        print(receiver)
        content = message['content']

        action_service.send_email(content,receiver)

    elif action_type =='sms':
        receiver = message['receiver_id']
        print(receiver)

        content = message['content']
        # print(type(mesg))
        action_service.send_sms(content,receiver)   

    elif action_type == 'control':
        sensor_info = message['sensor_info']
        control_producer("act_sen",sensor_info)

    else:
        print("Undefind Action")
        pass


def main():
    # t1 = threading.Thread(target=ca.actmanager_depmanager,args=(func))
    # t1.start()
    # t1.join()
    ca.actmanager_depmanager(func)

if __name__ == "__main__":
    main()
