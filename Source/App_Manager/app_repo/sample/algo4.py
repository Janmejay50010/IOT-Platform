import communication_api as capi
import threading
from math import sin, cos, sqrt, atan2, radians
import time
import action_api

PARAT = 'installedin'
PARA = ['barricade','bus']
THRESH = int(2000)
#Sensor Instance list
sensorinf = {}
#Sensor Data List
sensordata = {}
#Request for Instance list

creq,crep= capi.initiate_com()
print(creq,crep)
time.sleep(5)



def calc_dist(cord_1,cord_2):

    R = 6373.0
    x = cord_1.split(':')
    y = cord_2.split(':')
    lat1 = radians(int(x[0]))
    lon1 = radians(int(x[1]))
    lat2 = radians(int(y[0]))
    lon2 = radians(int(y[1]))

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    distance = R * c
    #print('Distance : ',distance)
    return distance



def fetch_sensorinf():
    for par in PARA: 
        req = {PARAT:par,'sensorType':'gps'}
        capi.app_senmanager(req, creq)
    dat = capi.senmanager_app(crep)
    for v in dat:
        par = 'barricade'
        sensorinf[par] = list()
        sensorinf[par] = v['topic']
        break
    for v in dat:
        par = 'bus'
        sensorinf[par] = list()
        sensorinf[par] = v['topic']
        break
        #print(v)

print("before sensor inf")

nthr= threading.Thread(target=fetch_sensorinf)
nthr.start()
time.sleep(2)

print(sensorinf)

def action_algo():
    for cord_bar in sensordata['barricade']:
        for cord_bus in sensordata['bus']:
            if calc_dist(sensordata['barricade'][cord_bar], sensordata['bus'][cord_bus]) <= THRESH:
                local_dist=calc_dist(sensordata['barricade'][cord_bar], sensordata['bus'][cord_bus])
                action_api.callback(cord_bus+' approaching '+cord_bar+' is '+str(local_dist)+' metres away')
                #print(sensordata)
                print(cord_bus+' approaching '+cord_bar,' is ',calc_dist(sensordata['barricade'][cord_bar], sensordata['bus'][cord_bus]),' metres away')
                #actionmanagerfunctioncall

def fetch_sensordata(inst,var):
    val = capi.get_sensor_data(inst)
    for v in val:
        #print(v)
        sensordata[var][v['instance']]=v['value']
        #print(v)



def take_action():
    for v in sensorinf:
        print(v)
        sensordata[v]={}
        for iv in sensorinf[v]:
            #print(iv)
            th = threading.Thread(target=fetch_sensordata,args=(iv,v))
            th.start()
    # to be uncommented in case interval scheduler does not work
    while True:
        time.sleep(5)
        action_algo()

take_action()
