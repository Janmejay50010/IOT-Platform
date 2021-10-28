import sys
import communication_api as capi
import threading
import time
import action_api as api
import json

# reque = {'locationId':'Sector2','sensorType': 'camera'}
with open('sensor.json') as f:
    sensor_request=json.load(f)
reque={'locationId':sensor_request['camera'][0],'sensorType': 'camera'}

creq,crep= capi.initiate_com()
#print(creq,crep)
time.sleep(5)
capi.app_senmanager(reque, creq)

def fetchtopic():
    val = capi.senmanager_app(crep)
    for v in val:
        for x in v['topic']:
            th = threading.Thread(target=fetchsensordata,args=(x,))
            th.start()


def dataprocess(dat):
    
    if int(dat) < 40:
        print('\nLow Congestion !!!\n')
        api.send_action('signal', sensor_request['camera'][0], '0')
    elif int(dat) < 70 and int(dat) >= 40:
        print('\nMedium Congestion !!!\n')
        api.send_action('signal', sensor_request['camera'][0], '1')
    else:
        print('\nHigh Congestion !!!\n')
        api.send_action('signal', sensor_request['camera'][0], '2')

def fetchsensordata(topi):
    #print('inside this')
    val = capi.get_sensor_data(topi)
    for v in val:
        if v['value'] != '':
            dataprocess(v['value'])



thr = threading.Thread(target=fetchtopic)
thr.start()