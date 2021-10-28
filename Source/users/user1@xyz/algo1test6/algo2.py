import sys
import communication_api as capi
import action_api as api
import threading
import time
import json
# reque1 = {'installedin':'bus','sensorType':'light'}
# reque2 = {'installedin':'bus','sensorType':'temperature'}

with open('sensor.json') as f:
    request_sensor=json.load(f)

reque1={'sensorType':'light','locationId':request_sensor['light'][0]}
reque2={'sensorType':'temperature','locationId':request_sensor['temperature'][0]}
#print(reque1)
#print(reque2)

#capi.depmanager_senmanager(reque1)
#capi.depmanager_senmanager(reque2)
sendat = {}
AC = 'ac'
LIGHT = 'light_con'
creq,crep= capi.initiate_com()
#print(creq,crep)
time.sleep(5)
capi.app_senmanager(reque1, creq)
capi.app_senmanager(reque2, creq)





vastream = list()
def fetchtopic():
    val = capi.senmanager_app(crep)
    for v in val:
        #vastream.clear()
        #print('line 17',v)
        vastream.append(v)
        for x in v['topic']:
            th = threading.Thread(target=fetchsensordata,args=(x,))
            sendat[x] = {}
            th.start()


def dataprocess1(dat):
    
    if int(dat) > 35:
        print('\nSwitch on the ac!!!',end='\n\n')
        api.send_action(AC, request_sensor['temperature'][0], '1')
    elif int(dat) < 23:
        print('\nSwitch off the ac!!!',end='\n\n')
        api.send_action(AC, request_sensor['temperature'][0], '0')
    

def dataprocess2(dat):

    if int(dat) <= 700:
        print('\nSwitch on the light!!!',end='\n\n')
        api.send_action(LIGHT, request_sensor['light'][0], '1')


def fetchsensordata(topi):
    #print('inside this')
    val = capi.get_sensor_data(topi)
    for v in val:
        if v['value'] != '':
            #sendat[topi].clear()
            #sendat[topi] = v
            if v['type'] == 'temperature':
                dataprocess1(v['value'])
            else:
                dataprocess2(v['value'])

thr = threading.Thread(target=fetchtopic)
thr.start()

time.sleep(5)
#print(vastream)

# for v in vastream:
#     for x in v['topic']:
#         print(x)
#         sendat[x] = { }
#         th = threading.Thread(target=fetchsensordata,args=(x,))
#         th.start()





# print(sendat)


# while True:
#     time.sleep(3)
#     for v in sendat:
#         #print(sendat[v])
#         if len(sendat[v])>0:
#             if sendat[v]['value'] != '':
#                 if sendat[v]['type'] == 'temperature':
#                     th = threading.Thread(target=dataprocess1,args=(sendat[v]['value'],))
#                     th.start()
#                 else:
#                     th = threading.Thread(target=dataprocess2,args=(sendat[v]['value'],))
#                     th.start()


#senconct('sensor1')
#senconct('sensor2')


# sensorname||sensorip:port sensorname||sensorip:port
# sensorname||sensorip:port
