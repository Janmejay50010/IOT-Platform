import json
import configparser
import datetime
config = configparser.ConfigParser()
config.read("Deployer.config")
class Node:
    def __init__(self, ip, port) -> None:
        self.Ip = ip
        if(isinstance(port, str)):
            port = int(port)
        self.Port = port
        self.HeartbeatInterval = 20
        self.HasFailed = False
        self.LastResponseTime = None
        self.StatusUrl = GetStatusEndPoint().format(ip, port)
        self.LoadUrl = GetLoadEndPoint().format(ip, port)
        self.DeployUrl = GetDeployUrl().format(ip, port)
        self.StopContainerUrl = GetEndPointUrl("StopContainerUrl").format(ip, port)
        self.GetAllContainersStatusUrl = GetEndPointUrl("GetAllContainersStatusUrl").format(ip, port)
        self.GetLogsUrl = GetEndPointUrl("GetLogsUrl").format(ip, port)
        self.RestartFailedContainerUrl = GetEndPointUrl("RestartFailedContainerUrl").format(ip, port)
    
    def __eq__(self, o: object) -> bool:
        return self.__dict__ == o.__dict__
    
class MongoDB:
    def __init__(self):
        self.MongoClient = None
        self.DatabaseName = None
        self.ActiveNodes = None
        self.LoadBalancing = None

def GetNodesSetting(nodeType):
    nodesJson = config["WorkerNodes"][nodeType]
    heartbeatInterval = config["WorkerNodes"]["HeartbeatInterval"]
    jsonObj = json.loads(nodesJson)
    workerNodes = []
    for node in jsonObj:
        newNode = Node(node["Ip"], node["Port"])
        newNode.HeartbeatInterval = heartbeatInterval
        workerNodes.append(newNode)
    return workerNodes

def GetMainWorkerNodes():
    nodes = GetNodesSetting("nodes")
    return nodes

def GetReserveWorkerNodes():
    nodes = GetNodesSetting("ReserveNodes")
    return nodes

def GetMongoDBSettings():
    dbObject = MongoDB()
    settings = config["MongoDB"]

    dbObject.MongoClient = settings["MongoClient"]
    dbObject.DatabaseName = settings["DatabaseName"]
    dbObject.ActiveNodes = settings["ActiveNodes"]
    dbObject.LoadBalancing = settings["LoadBalancingCol"]
    dbObject.Deployments = settings["Deployments"]
    dbObject.ContainerDetails = settings["ContainerDetails"]
    return dbObject

def GetStatusEndPoint():
    return config["WorkerNodes"]["StatusEndPoint"]

def GetLoadEndPoint():
    return config["WorkerNodes"]["LoadEndPoint"]

def GetNodeCheckInterval():
    return int(config["WorkerNodes"]["NodeCheckInterval"])

def GetLoadBalanceSettings():
    loadBalanceSetting = config["LoadBalancing"]
    ip = loadBalanceSetting["Ip"]
    port = loadBalanceSetting["Port"]
    url = loadBalanceSetting["Url"].format(ip, port)
    ipPort = {"Ip" : ip, "Port" : port, "Url" : url}
    return ipPort

def GetDeployUrl():
    return config["WorkerNodes"]["DeployUrl"]

def GetEndPointUrl(endPoint):
    return config["WorkerNodes"][endPoint]

def GetTopicToListenForDeployment():
    return config["Kafka.ServiceLifecycleManager"]["DeploymentTopic"]

def GetTopicLogRequest():
    return config["Kafka.ServiceLifecycleManager"]["RequestLogsTopic"]

def GetTopicLogResponse():
    return config["Kafka.ServiceLifecycleManager"]["ResponseLogsTopic"]

def GetTopicAbortContainer():
    return config["Kafka.ServiceLifecycleManager"]["AbortTopic"]


def GetKafkaBootStrapServer():
    return config["Kafka"]["bootStrapServer"]

def GetDeploymentResponseTopic():
    return config["Kafka.ServiceLifecycleManager"]["DeployResponseTopic"]

def GetMonitorFrequency():
    return int(config["monitoring"]["MonitorFrequency"])

if(__name__=="__main__"):
    GetMongoDBSettings()
