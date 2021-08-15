from logging import exception
import flask
import sys
import random
import json
from pathlib import Path
from flask.json import jsonify
import DockerHelper
from werkzeug.utils import secure_filename
import os
import zipfile
import pickle
app = flask.Flask(__name__)
app.config["DEBUG"] = True

ip = sys.argv[1]
port = int(sys.argv[2])
heartBeatInterval = int(sys.argv[3])


class Stats:
    def __init__(self) -> None:
        self.cpu = 0
        self.temperature = 0
        self.bandwidthUsage = 0
    
    def AssignRandom(self):
        self.cpu = random.randint(0, 100)
        self.temperature = random.randint(30, 90)
        self.bandwidthUsage = round(random.uniform(0, 1), 2)

class ContainerMetaData:
    def __init__(self) -> None:
        self.Id = None
        self.Status = None

class DynamicObj:
    pass

dockerHelper = DockerHelper.Docker()

@app.route('/', methods=['GET'])
def home():
    #print("adfadfadfasdfa")
    return ''' Hi, Ur Node Running At {}:{} is Up and active ....... have a naise day '''.format(ip, port)

@app.route('/Status', methods=['GET'])
def Status():
    #print("adfadfadfasdfa")
    returnObj = {"Ip" : ip, "Port" : port, "Status" : "Alive" }
    return json.dumps(returnObj)

@app.route("/GetLoadStats", methods=['GET'])
def GetLoadAndStats():
    currentStats = Stats()
    currentStats.AssignRandom()
    return  json.dumps(currentStats.__dict__)


@app.route("/Deploy", methods=['POST'])
def Deploy() :
    data = flask.request.form
    deployZip = flask.request.files['upload_zip']
    if(isinstance(data, str)):
        data = json.loads(data)
    #print(data)  
    #Implement Deployment using the data
    try:
        algoId = data["AlgorithmID"]
        algoName = data["AlgorithmName"]
        userName = data["User"]
        pathToStore = "./usersNode/{}".format(data["User"])
        Path(pathToStore).mkdir(parents=True, exist_ok=True)
        zipFilePath = os.path.join(pathToStore, secure_filename(deployZip.filename))
        deployZip.save(zipFilePath)
        with zipfile.ZipFile(zipFilePath, 'r') as zip_ref:
            zip_ref.extractall(pathToStore)
        containerId = dockerHelper.Deploy(algoId, algoName, userName)
        return flask.jsonify({"Status" : "OK", "ID" : containerId})  
    except Exception as e:
        print(e)
        return flask.jsonify({"Status" : "Failed", "error" : str(e)}) 


@app.route("/StopContainer", methods=['POST'])
def StopContainer() :
    data = flask.request.json
    if(isinstance(data, str)):
        data = json.loads(data)
    if(isinstance(data, bytes)):
        data = pickle.loads(data)
    #print(data)  
    #Implement Deployment using the data
    try:
        containerId = data["ContainerId"]
        container = dockerHelper.GetContainerById(containerId)
        if(container != None):
            container.stop()
        return flask.jsonify({"Status" : "OK", "ID" : containerId, "Message" : "Successfully Stopped the container"})  
    except Exception as e:
        print(e)
        return flask.jsonify({"Status" : "Failed", "error" : str(e)})  

@app.route("/CleanContainers", methods=['GET'])
def CleanContainers():
    deletedContainers = dockerHelper.PruneContainers()
    return jsonify(deletedContainers)

@app.route("/GetAllContainersStatus", methods=['GET'])
def GetContainers():
    allContainers = dockerHelper.GetAllContainers()
    #allContainers[0].attrs['State']
    combinedObj = []
    for container in allContainers:
        obj = container.attrs['State']
        obj["ContainerId"] = container.id
        combinedObj.append(obj)
    return jsonify(combinedObj)


@app.route("/GetLogs", methods=['post'])
def GetLogs():
    data = flask.request.json
    if(isinstance(data, str)):
        data = json.loads(data)
    #print(data)  
    #Implement Deployment using the data
    try:
        containerId = data["ContainerId"]
        container = dockerHelper.GetContainerById(containerId)
        if(container != None):
            logs = container.logs()
            return jsonify({"Status" : "OK", "ContainerId" : containerId, "logs" : logs.decode()})
    except Exception as e:
        print(e)
        return flask.jsonify({"Status" : "KO", "error" : str(e)})  

@app.route("/RestartFailedContainer", methods=['post'])
def RestartFailedContainers():
    try:
        data = pickle.loads(flask.request.data)
        failedContainers = dockerHelper.GetFailedContainers()
        excludedContainers = [x["ContainerId"] for x in data["Exclude"]]
        for container in failedContainers:
            if(container.id not in excludedContainers):
                container.restart()
        return jsonify({"Status" : "OK", "message" : "OK. Restarted all failed containers"})
    except Exception as e:
        return jsonify({"Status" :"KO",  "message" : str(e)})

@app.route("/StopAllAndPrune", methods=['Get'])
def StopAllAndPrune():
    try:
        containers = dockerHelper.GetAllContainers()
        for container in containers:
            #container.kill(signal = "SIGSTOP")
            #container.kill(signal = "SIGTERM")
            container.stop()
        
        return CleanContainers()
    except Exception as e:
        return jsonify({"Status" :"KO",  "message" : str(e)})

app.run(port=port, host=ip)
