import os
import shutil
import json
from typing import NoReturn

conf=dict()
with open("app_manager.config") as f:
    data=f.read().split('\n')
    for i in data:
        temp_conf=i.split(':')
        if len(temp_conf)>1:
            conf[temp_conf[0]]=temp_conf[1]

app_repo=conf['app_repo']
sensor_repo=conf['sensor_repo']
temp_files=conf['temp_files']
default_user_path=conf['default_user_path']
api_repo=conf['api_repo']

if not os.path.exists(default_user_path):
    os.mkdir(default_user_path)

def create_algo_in_repo(email,algo_id,base_algo,base_app,action_dict,sensor_conf=None):
    print(base_algo)
    if not os.path.exists(default_user_path+email):
        os.mkdir(default_user_path+email)
    dest_path=default_user_path+email+'/'+algo_id
    os.mkdir(dest_path)
    print(app_repo+base_app)
    for file in os.listdir(app_repo+base_app):
        #if os.path.isfile(file):
        shutil.copy(app_repo+base_app+'/'+file,dest_path)
    shutil.copy(app_repo+base_app+'/requirements.txt',dest_path)
    print("APIS")
    for file in os.listdir(api_repo):\
        shutil.copy(api_repo+file,dest_path)
    if action_dict is not None:
        with open(dest_path+'/action.json','w') as f:
            json.dump(action_dict,f)
    if sensor_conf is not None:
        with open(dest_path+'/sensor.json','w') as f:
            json.dump(sensor_conf,f)
    return shutil.make_archive(default_user_path+email+'/'+algo_id,'zip',default_user_path+email,algo_id)
