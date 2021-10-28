import sys
import communication_api as capi
import threading
import time
import ActionManagerSDK as AM
reque = {'locationId':'Sector_1'}

capi.depmanager_senmanager(reque)

sendat = {}

vastream = list()

def fetchtopic():
    val = capi.senmanager_depmanager_rep()
    for v in val:
        vastream.clear()
        vastream.append(v)

def fetchsensordata(topi):
    print('inside this')
    val = capi.get_sensor_data(topi)
    for v in val:
        sendat[topi].clear()
        sendat[topi] = v


thr = threading.Thread(target=fetchtopic)
thr.start()

time.sleep(5)
print(vastream)

for v in vastream:
    for x in v['topic']:
        sendat[x] = { }
        th = threading.Thread(target=fetchsensordata,args=(x,))
        th.start()


def dataprocess1(dat):
    
    if int(dat) <= 50:
        print('\nGood Air Quality !!!',end=' and ')
    elif int(dat) <= 100 and int(dat) > 50:
        print('\nModerate Air Quality !!!',end=' and ')
    elif int(dat) <= 150 and int(dat) > 101:
        print('\nUnhealthy for Sensitive Air Quality !!!',end=' and ')
    elif int(dat) <= 200 and int(dat) > 150:
        print('\nUnhealthy Air Quality !!!',end=' and ')
    elif int(dat) <= 300 and int(dat) > 200:
        print('\nVery Unhealthy Air Quality !!!',end=' and ')
    else:
        print('\nHazardous Air Quality !!!',end=' and ')
    AM.set_action('email','sachingoyal.0929@gmail.com','temperature','room1',90)

def dataprocess2(dat):
    
    if dat < 'MMMMM':
        print('Low Noise Pollution !!!\n')
    else:
        print('High Noise Pollution !!!\n')


print(sendat)


while True:
    time.sleep(3)
    for v in sendat:
        print(sendat[v])
        if len(sendat[v])>0:
            if sendat[v]['value'] != '':
                if sendat[v]['type'] == 'air':
                    th = threading.Thread(target=dataprocess1,args=(sendat[v]['value'],))
                    th.start()
                else:
                    th = threading.Thread(target=dataprocess2,args=(sendat[v]['value'],))
                    th.start()


#senconct('sensor1')
#senconct('sensor2')


# sensorname||sensorip:port sensorname||sensorip:port
# sensorname||sensorip:port
