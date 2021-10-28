import sys
import communication_api as capi
import threading
import time
import json

fixed_rate=5
iiit_cordinate="0:0"
# reque1 = {'locationId':'bus1','sensorType':'gps'}
# reque2 = {'locationId':'bus1','sensorType':'biometric'}

with open('sensor.json') as f:
    request_sensor=json.load(f)

reque1={'sensorType':'gps','locationId':request_sensor['gps'][0]}
reque2={'sensorType':'biometric','locationId':request_sensor['biometric'][0]}
#print(reque1)
#print(reque2)

# capi.depmanager_senmanager(reque1)
# capi.depmanager_senmanager(reque2)

creq,crep= capi.initiate_com()
#print(creq,crep)
time.sleep(5)
capi.app_senmanager(reque1, creq)
capi.app_senmanager(reque2, creq)


sendat = {}
vastream = list()

def dataprocess1(id,ins):
    for v in sendat:
        if len(sendat[v])>0:
            if sendat[v]['value']!='':
                if sendat[v]['type']=='gps' and sendat[v]['instance']==ins:
                    cordinate=sendat[v]['value']
                    s=cordinate.split(":")
                    x=s[0]
                    y=s[1]
                    result=(int(x)**2+int(y)**2)*fixed_rate
                    print('Fare of Person ',id,' is INR ',result)

def fetchtopic():
    val = capi.senmanager_app(crep)
    for v in val:
        #vastream.clear()
        #print('line 20',v)
        vastream.append(v)
        for x in v['topic']:
            th = threading.Thread(target=fetchsensordata,args=(x,))
            sendat[x] = {}
            th.start()

def fetchsensordata(topi):
    val = capi.get_sensor_data(topi)
    for v in val:
        if v['value'] != '':
            #sendat[topi].clear()
            #sendat[topi] = v
            if v['type'] == 'biometric':
                th = threading.Thread(target=dataprocess1,args=(v['value'],v['instance'],))
                th.start()
            else:
                sendat[topi].clear()
                sendat[topi] = v

thr = threading.Thread(target=fetchtopic)
thr.start()

# time.sleep(5)
# print(vastream)

# for v in vastream:
#     for x in v['topic']:
#         sendat[x] = { }
#         th = threading.Thread(target=fetchsensordata,args=(x,))
#         th.start()




#print(sendat)

# while True:
#     time.sleep(3)
#     for v in sendat:
#         #print(sendat[v])
#         if len(sendat[v])>0:
#             if sendat[v]['value'] != '':
#                 if sendat[v]['type'] == 'biometric':
#                     th = threading.Thread(target=dataprocess1,args=(sendat[v]['value'],sendat[v]['instance'],))
#                     th.start()
               
