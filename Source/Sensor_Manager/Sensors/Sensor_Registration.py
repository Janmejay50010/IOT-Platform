import json
import socket
import threading
from kafka import KafkaProducer
from pymongo import MongoClient
import monitoring_api as mon_api 
import communication_api as capi

global val 

kafkaipport = '52.15.89.83:9092'
#mongo_server="localhost:27017"
myclient = MongoClient("mongodb+srv://mudit:1234@cluster0.b7qsj.mongodb.net/myFirstDatabase?retryWrites=true")
db = myclient["json_db"]
type_registertion = db["type"]
instance_reg = db["instance"]
x = type_registertion.delete_many({})
x = instance_reg.delete_many({})

mon_api.monitor_thread("sensor_registration")

def start_sensor(ip_port,topic,sentype,loc):
    s=socket.socket()
    ip = ip_port.split(":")[0]
    port = int(ip_port.split(":")[1])
   
    s.connect((ip,port))
    producer = KafkaProducer(
        bootstrap_servers=[kafkaipport],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    print("topic on which sensor started",topic)
    while(1):
        mess=s.recv(4096).decode()
        if mess != '':
            print(mess)
            data = {'type':sentype,'value':mess,'instance':loc} 
            producer.send(topic, value=data)
            producer.flush()
  
def fetch_json():
    val = capi.senmanager_appmanager()
    valist = []
    #val = {}
    #val['se_type'] = 'instance_reg'
    #sensor_config_file = open("instance_reg.json", "r")
    #val['se_data'] = json.load(sensor_config_file)
    # for v in val:
    #     valist.append(v)
    #     print(v)
    for v in val:
        if v != '':
            if v['se_type'] == 'type_registration':
                json_object = v['se_data']
                for i in range(len(json_object)):
                    val = json_object["sensor"+str(i+1)]
                    # print(val)
                    type_registertion.insert_one(val) 
            else:
                json_object = v['se_data']
                #print(json_object)
                for i in range(len(json_object)):
                    inner_object=json_object["sensor"+str(i+1)]                    
                    inner_object["topic"]=inner_object["sensorType"]+inner_object['locationId']+"id"+str(i)
                    instance_reg.insert_one(inner_object) 
                    if(inner_object["purpose"]=="data-generator"):
                        x=threading.Thread(target=start_sensor,args=(inner_object["sensor_ip"],inner_object["topic"],inner_object["sensorType"],inner_object["locationId"],))
                        x.start() 
                    


sjth = threading.Thread(target=fetch_json)
sjth.start()




#sensor_config_file = open("instance_reg.json", "r")
#json_object = json.load(sensor_config_file)

# for v in SEN_JSON:
#     if v['se_type'] == 'type_registration':
#         json_object = v['se_data']
#         for i in range(len(json_object)):
#             val = json_object["sensor"+str(i+1)]
#             # print(val)
#             type_registertion.insert_one(val) 
#     else:
#         json_object = v['se_data']
#         for i in range(len(json_object)):
#             inner_object=json_object["sensor"+str(i+1)]
#             ip_object=inner_object["sensor_ip"]
#             inner_object["topic"]=inner_object["sensorType"]+inner_object['locationId']+"id"+str(i)
#             instance_reg.insert_one(inner_object) 
#             x=threading.Thread(target=start_sensor,args=(inner_object["sensor_ip"],inner_object["topic"],inner_object["sensorType"],))
#             x.start() 


#sensor_config_file.close()


