import communication_api as ca 
import threading
import time
import datetime
import json
import monitoring_db
from os import system
import subprocess

threshold = 10

dic = {

}


def fetch_status():
    for message in ca.monitor():
        dic[message['moduleName']]=message['currentTime']

def fun(module_name,instance_ip,file_path,file_name):
    cmd = "python3 ssh.py %s %s %s %s"%(module_name,instance_ip,file_path,file_name)
    subprocess.Popen(cmd,shell=True)



if __name__ == '__main__':
    t = threading.Thread(target=fetch_status)
    t.daemon=True
    t.start()

    print('Monitoring Started...')
    # status = {
    #     "Sensor-Manager":"down","App-Manager":"down","Action-Manager":"down","Deployer":"down",
    #     "Scheduler":"down"
    #     }
    status = {

    }
    down = []
    with open('config.json') as f:
        data = json.load(f)
    services = data['service']
    print(services)
    while True:
        currentTime = datetime.datetime.utcnow()
        print(dic)
        if len(dic)>0:
            for module in dic.keys():
                date_time_obj = datetime.datetime.strptime(dic[module], '%Y-%m-%d %H:%M:%S.%f')
                diff = (currentTime - date_time_obj).seconds
                print("Difference:",diff)
                if diff > threshold:
                    if module in down:
                        continue
                    else:
                        print(module+" down")
                        status[module]= 'down'
                        fun(module,services[module]['server'],services[module]['path'],services[module]['file'])
                        down.append(module)
                else:
                    if module in down:
                        down.remove(module)
                    print(module+" up")
                    status[module]= 'up'
                monitoring_db.update_component_status(module,status[module])
        time.sleep(5)
