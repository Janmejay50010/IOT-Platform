import zipfile
import os
import shutil
import json
import jsonschema

app_config_schema={
    "type":"object",
    "properties":{
        "ApplicationName":{
            "type":"string"
        },
        "Dependencies":{
            "type":"array",
            "items":{
                "type":"string"
            }
        },
        "Algorithm":{
            "type":"array",
            "items":{
                "type":"object",
                "properties":{
                    "AlgorithmName":{
                        "type":"string"
                    },
                    "sensors":{ 
                        "type":"array",
                        "items":{"type":"string"}
                    },
                    "sensor_num":{
                        "type":"array",
                        "items":{"type":["number","string"]}
                    },
                    "action":{
                        "type":"array",
                        "items":{"type":"string"}
                    }
                }
            },
        }
    }
}

sensor_inst_schema={
    "type":"object",
    "properties":{
        "sensorType":{"type":"string"},
        "location":{
            "type":"object",
            "properties":{        
                "lat":{"type":"string"},
                "longitude":{"type":"string"},
            }
        },
        "locationId":{"type":"string"},
        "address":{"type":"string"},
        "sensor_ip":{"type":"string"},
    },
    "required":["sensorType","locationId"]
}

sensor_type_schema={
    "type":"object",
    "properties":{
        "sensorType":{"type":"string"},
        "input_type":{"type":"string"},
        "output_type":{"type":"string"},
        "data_size":{"type":"string"},
        "data_rate":{"type":"string"},
        
    },
    "required": ["sensorType","output_type","data_size","data_rate"]
}

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

def validate_sensor_type_config(json_path,file_name):
    if not os.path.exists(sensor_repo):
        os.mkdir(sensor_repo)
    try:
        with open(json_path,'r') as fp:
            sensor_json=json.load(fp)
        for i in sensor_json:
            jsonschema.validate(sensor_json[i],schema=sensor_type_schema)
            
        shutil.copyfile(json_path,sensor_repo+file_name)
        os.remove(json_path)
        return True
    except:
        os.remove(json_path)
        return False

def validate_sensor_instance_config(json_path,file_name):
    if not os.path.exists(sensor_repo):
        os.mkdir(sensor_repo)
    try:
        with open(json_path,'r') as fp:
            sensor_json=json.load(fp)
        #print(sensor_json)
        for i in sensor_json:
            jsonschema.validate(sensor_json[i],schema=sensor_inst_schema)
        shutil.copyfile(json_path,sensor_repo+file_name)
        os.remove(json_path)
        sensor_list=[{'sen_type':sensor_json[i]['sensorType'], 'sen_loc':sensor_json[i]['locationId']} for i in sensor_json]
        return True,sensor_list
    except:
        os.remove(json_path)
        return False,None

def validate_app_config(json_path):
    with open(json_path,'r') as fp:
        sensor_json=json.load(fp)
    jsonschema.validate(sensor_json,schema=app_config_schema)

def extract_algos(config_json):
    algo_list=config_json['Algorithm']
    algo_list=[{'AlgorithmName':algo['AlgorithmName'],'sensors':algo['sensors'],'sensor_num':algo['sensor_num'],'action':algo['action']} for algo in algo_list]
    return algo_list

def validate_app(zip_path,apps):
    zip_path=zip_path.replace('\\','/')
    file_name=zip_path.split('/')[-1].split('.')[:-1]
    file_name=''.join(file_name)
    print(apps)
    if apps==-1 :
        pass
    else:
        if file_name in apps:
            os.remove(zip_path)
            return False,None,None
    try:
        with zipfile.ZipFile(zip_path,'r') as zip:
            zip.extractall(temp_files)
    except:
        os.remove(zip_path)
        return False,None,None

    if os.path.exists(temp_files+file_name):
        try:
            validate_app_config(temp_files+file_name+'/app_config.json')
        except Exception as e:
            print(type(e).__name__)
            os.remove(zip_path)
            shutil.rmtree(temp_files+file_name)
            return False,None,None

        #creating requirements.txt
        with open(temp_files+file_name+'/app_config.json','r') as fp:
            app_config=json.load(fp)
        app_dependencies=app_config['Dependencies']
        with open(temp_files+file_name+'/requirements.txt','w') as fp:
            for dependency in app_dependencies:
                fp.write("%s\n"%dependency)
            fp.write("kafka-python\n")
        #extracting algos from json file
        print(app_config)
        algo_list=extract_algos(app_config)

        #copying app to repo
        if os.path.exists(app_repo+file_name):
            shutil.rmtree(app_repo+file_name)
        shutil.move(temp_files+file_name,app_repo+file_name)
        os.remove(zip_path)

    return True,file_name,algo_list