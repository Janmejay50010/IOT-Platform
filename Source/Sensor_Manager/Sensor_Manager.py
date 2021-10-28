import socket
import threading
import json
from kafka import KafkaProducer
import communication_api 
from pymongo import MongoClient 
import time
import monitoring_api as mon_api

required_sensor_data = []
request_list = []
action_list = []
com_topiclist = []
mongo_server="mongodb+srv://mudit:1234@cluster0.b7qsj.mongodb.net/myFirstDatabase?retryWrites=true"
myclient = MongoClient(mongo_server)
db = myclient["json_db"]
Collection = db["instance"]




def find_topics(data_dict):
    print("data dict",data_dict)
    global required_sensor_data
    required_sensor_data = Collection.find(data_dict)
    #print(required_sensor_data)
    #for v in required_sensor_data:
        #print('line28 : ',v)


def fetchcomtopic():
    val = communication_api.senmanager_depmanager()
    for v in val:
        #request_list.clear()
        print(v)
        #com_topiclist.append(v['topic'])
        thr = threading.Thread(target=fetchrequest,args=(v['topic'],))
        thr.start() 
        #print(com_topiclist)


thr_comt = threading.Thread(target=fetchcomtopic)
thr_comt.start() 
mon_api.monitor_thread("sensor_manager")
#print(com_topiclist)

def fetchrequest(topicc):
    valr = communication_api.senmanager_app(topicc+'_req')
    for v in valr:
        #request_list.clear()
        print(v)
        tdict = {}
        tdict['topic'] = topicc
        tdict['request'] = v
        request_list.append(tdict)
        topic = list()
        reptopic = topicc+'_rep'
        print(reptopic)
        val = v
        print('line 44 : ',val)
        find_topics(val)
        print("req data")
        global required_sensor_data
        #for v in required_sensor_data:
            #print('line64 : ',v)
        for sensor_data in required_sensor_data:
            # print("inside this loop")
            print("l recefived",sensor_data)
            topic.append(sensor_data["topic"])
        print(topic)
        if topic:   
            print("sending topic",topic)
            communication_api.app_senmanager({"topic":topic}, reptopic)

def takeaction():
    val = communication_api.senmanager_actmanager()
    print(val)
    for v in val:
        #action_list.clear()
        action_list.append(v)
        #print('line 44 : ',val)
        val = {}
        val['sensorType'] = v['sensorType']
        val['locationId'] = v['locationId']
        find_topics(val)
        print("req data")
        global required_sensor_data
        for sensor_data in required_sensor_data:
            # print("inside this loop")
            print("l recefived",sensor_data)
            ip_port = sensor_data["sensor_ip"]
            #print(ip_port)
            s=socket.socket()
            ip = ip_port.split(":")[0]
            port = int(ip_port.split(":")[1])
            s.connect((ip,port))
            print(ip_port)
            s.sendall(v['action'].encode())
            s.close()       


    

thr_act = threading.Thread(target=takeaction)
thr_act.start() 

    
# def req_hndler():
#     while True:
#         time.sleep(2)
#         topic=[]
#         for v in request_list:
#             topic.clear()
#             reptopic = v['topic']+'_rep'
#             val = v['request']
#             print('line 44 : ',val)
#             find_topics(val)
#             print("req data")
#             global required_sensor_data
#             #for v in required_sensor_data:
#                 #print('line64 : ',v)
#             for sensor_data in required_sensor_data:
#                 # print("inside this loop")
#                 print("l recefived",sensor_data)
#                 topic.append(sensor_data["topic"])
#             print(topic)
#             if topic:   
#                 print("sending topic",topic)
#                 communication_api.app_senmanager({"topic":topic}, reptopic)
#                 #communication_api.depmanager_senmanager_rep({"topic":topic})
#         request_list.clear()
        

# def control_hndler():
#     while True:
#         time.sleep(2)
#         for v in action_list:
#             val = {}
#             print('line 44 : ',val)
#             val['sensorType'] = v['sensorType']
#             val['locationId'] = v['locationId']
#             find_topics(val)
#             print("req data")
#             global required_sensor_data
#             for sensor_data in required_sensor_data:
#                 # print("inside this loop")
#                 print("l recefived",sensor_data)
#                 ip_port = sensor_data["sensor_ip"]
#                 #print(ip_port)
#                 s=socket.socket()
#                 ip = ip_port.split(":")[0]
#                 port = int(ip_port.split(":")[1])
#                 s.connect((ip,port))
#                 print(ip_port)
#                 s.sendall(v['action'].encode())
#                 s.close()
#             action_list.clear()

# thr_req = threading.Thread(target=req_hndler)
# thr_req.start() 

# thr_cont = threading.Thread(target=control_hndler)
# thr_cont.start() 
