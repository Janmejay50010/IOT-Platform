import sys
import communication_api as capi
import threading
import time
import action_api as api
radius=10
reque = {'sensorType':'gps','installedin':'bus'}
CONTR = 'buzzer'
#capi.depmanager_senmanager(reque)


sendat = {}
creq,crep= capi.initiate_com()
#print(creq,crep)
time.sleep(5)
capi.app_senmanager(reque, creq)

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
        
        break

def fetchsensordata(topi):
    val = capi.get_sensor_data(topi)
    for v in val:
        if v['value'] != '':
            sendat[topi].clear()
            sendat[topi] = v
        break

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


bus_cordinates=[]
bus_instance=[]
time.sleep(5)
for v in sendat:
    #print(sendat[v])
    if len(sendat[v])>0:
        if sendat[v]['value'] != '':
            bus_cordinates.append(sendat[v]['value'])
            bus_instance.append(sendat[v]['instance'])

#print((bus_cordinates,bus_instance))       
for i in range(len(bus_cordinates)-2):
    set_for_ins= set()
    count=0 
    cor1=bus_cordinates[i].split(":")
    x1=cor1[0]
    y1=cor1[1]
    for j in range(i+1,len(bus_cordinates)):    
        cor2=bus_cordinates[j].split(":")
        x2=cor2[0]
        y2=cor2[1]
        #print("dist",(int(x1)-int(x2))**2 + (int(y1)-int(y2))**2)
        if((int(x1)-int(x2))**2 + (int(y1)-int(y2))**2) < ((radius)**2):
            count=count+1
            set_for_ins.add(bus_instance[j])
    if(count>=2):
        #print(set_for_ins)
        for v in set_for_ins:
            api.send_action(CONTR, v, '1')      






               
