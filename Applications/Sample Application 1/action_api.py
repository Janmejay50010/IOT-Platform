# import sys
from kafka import KafkaProducer
import json


# sys.path.append('../communication_module')
# import communication_api as ca
# import prodapi

deptoact = { 'action_type': None}

sensor_info = {'sensor_type':None,'location_id':None,'action':None}


# message['action']=='email' or 'sms'

# message['action']=='control'

kafkaipport='localhost:9092'

producer = KafkaProducer(bootstrap_servers=kafkaipport,
value_serializer=lambda v: json.dumps(v).encode('utf-8'))  
def send_message(topic,message):
    producer.send(topic, value = message)
    producer.flush()

## need to remove sensor info and add single string variable named data

def send_notification(action_type,receiver_id,content):
    deptoact['action_type']=action_type
    deptoact['receiver_id']=receiver_id
    deptoact['content']=content
    producer.send('dep_act',value=deptoact)
    producer.flush()
    #call to action manager

def send_email(message):
    with open('action.json') as f:
        receiver_id=json.load(f)
    email_id=receiver_id['email']
    send_notification('email',email_id,message)

def send_sms(message):
    with open('action.json') as f:
        receiver_id=json.load(f)
    phone_no=receiver_id['sms']
    send_notification('sms',phone_no,message)

def callback(message):
    with open('action.json') as f:
        data=json.load(f)
    ## need work
    import subprocess
    subprocess.run(["python3",data['callback']+".py",message])


# def send_action(sensor_type,location_id,action):
#     deptoact['action_type'] = 'control'
#     sensor_info['sensor_type'] = sensor_type
#     sensor_info['location_id'] = sensor_location
#     sensor_info['action'] = sensor_value
#     deptoact['sensor_info'] = sensor_info
#     send_message('dep_act',deptoact)
def send_action(sensor_type,location_id,action):
    deptoact['action_type'] = 'control'
    sensor_info['sensorType'] = sensor_type
    sensor_info['locationId'] = location_id
    sensor_info['action'] = action
    deptoact['sensor_info'] = sensor_info
    send_message('dep_act',deptoact)

#set_action('email','sachingoyal.0929@gmail.com','temperature','room1',90)
# send_email("Hello World!!!!!")
# send_sms("hello World!!!")
