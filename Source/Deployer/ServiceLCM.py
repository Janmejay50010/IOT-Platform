from logging import Logger, exception
#from typing import ClassVar

import requests
import ConfigHelper
import json
from kafka.consumer import KafkaConsumer
from kafka import KafkaProducer
import threading
import Logger
import pymongo
from bson import json_util 
import sched
import time
import sys
import os
import zipfile
import pickle
from monitoring_api import monitor_thread


topicToListenForDeployment = ConfigHelper.GetTopicToListenForDeployment()
topicAbortContainer = ConfigHelper.GetTopicAbortContainer()
kafkaBootStrapServer = ConfigHelper.GetKafkaBootStrapServer()
topicLogRequests = ConfigHelper.GetTopicLogRequest()
topicLogResponse = ConfigHelper.GetTopicLogResponse()
schMonitor = sched.scheduler(time.time, time.sleep)
schFaultTolerance = sched.scheduler(time.time, time.sleep)
class DeploymentDetails:
    def __init__(self):
        self.NodeDetails = None
        self.ID = None
        self.ContainerId = None
        self.ExecutionStatus = -1
        self.RestartCount = 0
        self.Request = None
        self.ApplicationId = None
        self.UserName = None
class MongoDbDeploymentHelper:
    def __init__(self):
        self.connectionSettings = ConfigHelper.GetMongoDBSettings()
        self.myclient = pymongo.MongoClient(self.connectionSettings.MongoClient)
        self.mydb = self.myclient[self.connectionSettings.DatabaseName]
        self.DeploymentDB = self.mydb[self.connectionSettings.Deployments]
        self.ContainerDetails = self.mydb[self.connectionSettings.ContainerDetails]
        #self.ContainerDetails.drop()
        #self.DeploymentDB.drop()
        self.ActiveNodes =  self.mydb[self.connectionSettings.ActiveNodes]
        self.NodeDetails = None
        self.dbUri = self.connectionSettings.MongoClient
        self.SetDeploymentId()

    def SetDeploymentId(self):
        all = self.DeploymentDB.find()
        self.SetId = all.retrieved + 1
    
    def InsertRecord(self, deploymentDetails):
        deploymentDetails.ID = self.SetId
        self.SetId += 1
        SerializedObj = deploymentDetails.__dict__
        self.DeploymentDB.insert_one(SerializedObj)
        return True
    
    def get_mongo_collection_client(self, uri , db = "IAS_Project", collection_name = "servers"):
        try:
            servers = None
            client = pymongo.MongoClient(uri)
            db = client[db]
            servers = db[collection_name]
        except Exception as e:
            print(e)
        return servers

    # def get_servers(self):
    #     # with open(server_data_file_path, "r") as file:
    #     #     servers_data = json.load(file)
    #     #     for server_name in servers_data.keys():
    #     #         server = Server(**servers_data[server_name])
    #     #         self.servers.append(server)
    #     collection_client = self.get_mongo_collection_client(self.dbUri)
    #     cursor = collection_client.find(filter = {})
    #     for server_info in cursor:
    #         try:
    #             server_info.pop('_id')
    #             #server = Server(**server_info)
    #             #self.servers.append(server)
            
    #         except Exception as e:
    #             raise e
    

class dynamicObj:
    pass  



    
class ServiceLCM:
    def __init__(self):
        self.DeployConsumer = KafkaConsumer(
            topicToListenForDeployment,
            bootstrap_servers=[kafkaBootStrapServer],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='my-group',
            value_deserializer=lambda x: x.decode('utf-8')
        )
        
        self.AbortConsumer = KafkaConsumer(
            topicAbortContainer,
            bootstrap_servers=[kafkaBootStrapServer],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='my-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')))
        
        self.LogRequester = KafkaConsumer(
            topicLogRequests,
            bootstrap_servers=[kafkaBootStrapServer],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='my-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')))
        
        #self.LogResponse = KafkaProducer(bootstrap_servers=[kafkaBootStrapServer],value_serializer=lambda x: json.dumps(x).encode('utf-8'))
        
        self.DeployResponse = KafkaProducer(bootstrap_servers=[kafkaBootStrapServer],value_serializer=lambda x: x.encode('utf-8'))
        #self.LogResponse = KafkaProducer(bootstrap_servers=[kafkaBootStrapServer],value_serializer=lambda x: x.encode('utf-8'))
        self.SetId = None
        self.DeploymentResponseTopic = ConfigHelper.GetDeploymentResponseTopic()
        self.MongoDeploymentHelper = MongoDbDeploymentHelper()
        self.LoadBalancer = ConfigHelper.GetLoadBalanceSettings()
        self.lock = threading.Lock()
        self.MonitorFrequency = ConfigHelper.GetMonitorFrequency()
        self.EndPointPort = 40000
        self.path_to_app_repo = "../users/"

    def InitializeMonitoringAndFaultTolerance(self):
        while(True):
            schMonitor.enter(self.MonitorFrequency, 1, self.UpdateStatusOfContainers)
            schFaultTolerance.enter(self.MonitorFrequency, 1, self.FaultTolerance)
            schMonitor.run() 
            schFaultTolerance.run()
     

    def ListenToDeployments(self):
        print("Listening to Deployment Requests on kafka topic")
        for message in self.DeployConsumer:
            self.DeployConsumer.commit_async()
            yield message

    def ListenToStopRunningContainer(self):
        print("Listening to Stop Requests on kafka topic")
        for message in self.AbortConsumer:
            self.AbortConsumer.commit_async()
            yield message

    def ListenToLogRequests(self):
        print("Listening to Log Requests on kafka topic")
        for message in self.LogRequester:
            self.LogRequester.commit_async()
            yield message

    def DeployRequest(self):
        obj = self.ListenToDeployments()
        while(True):
            deploymentRequest = next(obj)
            Logger.Log(deploymentRequest.value)
            deployTask = threading.Thread(target = self.StartDeployment, args=(deploymentRequest,))
            deployTask.start()
    
    def GetWorkerNodes(self):
        collection = self.MongoDeploymentHelper.get_mongo_collection_client(self.MongoDeploymentHelper.dbUri)
        if(collection != None):
            return collection.find({"PlatformService" : False, "Status" : "running"})
        return []

    def StopRequest(self):
        obj = self.ListenToStopRunningContainer()
        while(True):
            stopRequest = next(obj)
            Logger.Log(stopRequest.value)
            stopTask = threading.Thread(target = self.StopDeployment, args=(stopRequest,))
            stopTask.start()

    def LogRequest(self):
        obj = self.ListenToLogRequests()
        while(True):
            logRequests = next(obj)
            Logger.Log(logRequests.value)
            LogTask = threading.Thread(target = self.GetLogs, args=(logRequests,))
            LogTask.start()

    def UpdateStatusOfContainers(self):
        nodes = self.GetWorkerNodes()
        for node in nodes:
            try:
                response = requests.request(url = self.GetAllContainersStatusUrl(node), method = 'GET')
                if(response.ok):
                    data = json.loads(response.content)
                    for container in data:
                        containerId = container['ContainerId']
                        container["NodeDetails"] = node
                        existingContainers = self.MongoDeploymentHelper.ContainerDetails.find_one({'ContainerId' : containerId})
                        if(existingContainers != None):
                            if("Status" in existingContainers.keys() and existingContainers["Status"] == "Interrupted"):
                                container["Status"] = "Interrupted"
                            self.MongoDeploymentHelper.ContainerDetails.update_one(
                            { "ContainerId": containerId },
                            { "$set": container }
                            )
                        deploymentDetails = self.MongoDeploymentHelper.DeploymentDB.find_one({'ContainerId' : containerId})
                        if(deploymentDetails != None):
                            deploymentDetails["NodeDetails"] = container["NodeDetails"]
                            self.MongoDeploymentHelper.DeploymentDB.update_one(
                            { "ContainerId": containerId },
                            { "$set":  deploymentDetails}
                            )
                        else:
                            #self.MongoDeploymentHelper.ContainerDetails.insert_one(container)
                            pass
                else:
                    #Handle Node internal server error
                    pass
                
            except Exception as e:
                #Node is down
                print(e)
        

    def FaultTolerance(self):
        for node in self.GetWorkerNodes():
            try:
                excludeContainers = list(self.MongoDeploymentHelper.ContainerDetails.find({'Status' : "Interrupted"}))
                response = requests.request(url = self.RestartFailedContainerUrl(node), data = pickle.dumps({"Exclude" : excludeContainers}) ,method = 'POST')
            except Exception as e:
                #Node is down
                print(e)    


    

    def StartListening(self):
        deployThread = threading.Thread(target=self.DeployRequest)
        deployThread.start()
        monitorAndFaultTOlerance = threading.Thread(target = self.InitializeMonitoringAndFaultTolerance)
        monitorAndFaultTOlerance.start()
        logRequests = threading.Thread(target = self.LogRequest)
        logRequests.start()
        self.StopRequest()
    
    def GetLogs(self, requestKafka):
        try:
            ackNowledgement = {"Status" : "KO", "Message" : "GetLogs Failed"}
            request = requestKafka.value
            request["UserName"] = request["User"]
            request["ApplicationId"] = request["AlgorithmID"]
            existingContainers = self.MongoDeploymentHelper.ContainerDetails.find_one({'UserName' : request["UserName"], 'ApplicationId' : request["ApplicationId"]})
            if(existingContainers != None):
                containerId = existingContainers["ContainerId"]
                nodeDetails = self.MongoDeploymentHelper.DeploymentDB.find_one({"ContainerId" : containerId})
                if(nodeDetails != None):
                    node = nodeDetails["NodeDetails"]
                    logUrl = self.GetLogsUrl(node)
                    response = requests.post(logUrl, json = {"ContainerId" : containerId})
                    if(response.ok):
                        responseJson = json.loads(response.content)
                        if(responseJson["Status"] != "OK"):
                            raise Exception("Internal Server Error" + response["error"])
                        ackNowledgement["Status"] = "OK"
                        ackNowledgement["Message"] = responseJson["logs"]
        except Exception as e:
            print(e)
        ackJson = json.dumps(ackNowledgement)
        print("Sending Response : " , ackJson)
        self.DeployResponse.send(topicLogResponse, value = ackJson) 
        self.DeployResponse.flush()   


    def StopDeployment(self, requestKafka):
        try:
            request = requestKafka.value
            request["UserName"] = request["User"]
            request["ApplicationId"] = request["AlgorithmID"]
            existingContainers = self.MongoDeploymentHelper.ContainerDetails.find_one({'UserName' : request["UserName"], 'ApplicationId' : request["ApplicationId"]})
            ackNowledgement = {"Status" : "KO", "Message" : "StopContainer Failed"}
            if(existingContainers != None):
                containerId = existingContainers["ContainerId"]
                nodeDetails = self.MongoDeploymentHelper.DeploymentDB.find_one({"ContainerId" : containerId})
                if(nodeDetails != None):
                    node = nodeDetails["NodeDetails"]
                    stopUrl = self.StopContainerUrl(node)
                    response = requests.post(stopUrl, json = {"ContainerId" : containerId})
                    if(response.ok):
                        ackNowledgement["Status"] = "OK"
                        ackNowledgement["Message"] = "Stopped container with id " + containerId + " Successfully"
                        existingContainers["Status"] = "Interrupted"
                        self.MongoDeploymentHelper.ContainerDetails.update_one(
                            { "ContainerId": containerId },
                            { "$set": existingContainers }
                            )

        except Exception as e:
            ackNowledgement["Message"] = str(e)
        ackJson = json.dumps(ackNowledgement)
        print("Sending Response : " , ackJson)
        self.DeployResponse.send(topicLogResponse, value = ackJson)
        self.DeployResponse.flush()

    def GetLoadBalancedNode(self):
        collection = self.MongoDeploymentHelper.get_mongo_collection_client(self.MongoDeploymentHelper.dbUri)
        if(collection != None):
            nodes = collection.find(filter = {"Status" : "running", "PlatformService" : False})
        else:
            nodes = []
        minCpuNode = None
        minCpu = sys.maxsize
        for node in nodes:
            if(node["Metrics"]["CPUUtilization"] < minCpu):
                minCpu = node["Metrics"]["CPUUtilization"]
                minCpuNode = node
        return minCpuNode
    
    def GetDeployUrl(self, node):
        if(node == None):
            return ""
        url = "http://{}:{}/Deploy".format(node["IpAddr"], self.EndPointPort)
        return url

    def GetAllContainersStatusUrl(self, node):
        if(node == None):
            return ""
        url = "http://{}:{}/GetAllContainersStatus".format(node["IpAddr"], self.EndPointPort)
        return url
    
    def RestartFailedContainerUrl(self, node):
        if(node == None):
            return ""
        url = "http://{}:{}/RestartFailedContainer".format(node["IpAddr"], self.EndPointPort)
        return url
    
    def StopContainerUrl(self, node):
        if(node == None):
            return ""
        url = "http://{}:{}/StopContainer".format(node["IpAddr"], self.EndPointPort)
        return url

    def GetLogsUrl(self, node):
        if(node == None):
            return ""
        url = "http://{}:{}/GetLogs".format(node["IpAddr"], self.EndPointPort)
        return url

    
    # def GetDeployUrl(self, node):
    #     if(node == None):
    #         return ""
    #     url = "http://{}:{}/Deploy".format("127.10.10.10", self.EndPointPort)
    #     return url

    # def GetAllContainersStatusUrl(self, node):
    #     if(node == None):
    #         return ""
    #     url = "http://{}:{}/GetAllContainersStatus".format("127.10.10.10", self.EndPointPort)
    #     return url
    
    # def RestartFailedContainerUrl(self, node):
    #     if(node == None):
    #         return ""
    #     url = "http://{}:{}/RestartFailedContainer".format("127.10.10.10", self.EndPointPort)
    #     return url
    
    # def StopContainerUrl(self, node, test = True):
    #     if(node == None):
    #         return ""
    #     url = "http://{}:{}/StopContainer".format("127.10.10.10", self.EndPointPort)
    #     return url
    # def GetLogsUrl(self, node):
    #     if(node == None):
    #         return ""
    #     url = "http://{}:{}/GetLogs".format("127.10.10.10", self.EndPointPort)
    #     return url

    def StartDeployment(self, request):
        # loadBalanceUrl = self.LoadBalancer["Url"]
        # Logger.Log("Hitting {} to get the stats of Worker Nodes".format(loadBalanceUrl))
        # response = None
        # try:
        #     response = requests.get(loadBalanceUrl)
        # except Exception as e: #Handle Load balancer offline
        #     raise e
        # if(not response.ok):
        #     #Handle Internal server error in load balancer
        #     pass
        # Logger.Log(response.text)
        # responseObj = json.loads(response.text)
        # if(responseObj["Node"] == "None"):
        #     #All Nodes are Down ....... No Node to Deploy
        #     #Handle it
        #     print("No Nodes are available to deploy")
        #     return
        # nodeToDeploy = json_util.loads(responseObj["Node"])
        #nodeToDeploy = {"DeployUrl" : "Asdf"}
        nodeToDeploy = self.GetLoadBalancedNode()
        try:
            deployUrl = self.GetDeployUrl(nodeToDeploy)
            postData = request.value
            #zipf = zipfile.ZipFile('Python.zip', 'w', zipfile.ZIP_DEFLATED)
            #ZipHelper.zipdir('tmp/', zipf)
            #zipf.close()
            postDataJson = json.loads(postData)
            userName = postDataJson["User"]
            algoId = postDataJson["AlgorithmID"]
            app_path = self.path_to_app_repo + userName + "/" + algoId + ".zip"
            file = {'upload_zip': open(app_path,'rb')}
            print("Posting data ", postData)
            response = requests.post(deployUrl, data = postDataJson, files=file)
            if(response.ok):
                jsonResponse = json.loads(response.text)
                if(jsonResponse["Status"] == "OK"):
                    #STORE DEPLOYMENT DETAILS IN MONGO DB
                    self.lock.acquire()
                    decodedJson = json.loads(postData)
                    userName = decodedJson["User"]
                    applicationInstanceId = decodedJson["AlgorithmID"]
                    deploymentDetails = DeploymentDetails()
                    deploymentDetails.ContainerId = jsonResponse["ID"]
                    deploymentDetails.NodeDetails = nodeToDeploy
                    deploymentDetails.Request = json_util.loads(postData)
                    deploymentDetails.UserName = userName
                    deploymentDetails.ApplicationId = userName
                    self.MongoDeploymentHelper.InsertRecord(deploymentDetails)
                    self.MongoDeploymentHelper.ContainerDetails.insert_one({"UserName" : userName, "ApplicationId" : applicationInstanceId, "ContainerId" : deploymentDetails.ContainerId})
                    self.lock.release()
                    obj = dynamicObj()
                    obj.request = request.value
                    obj.response = deploymentDetails.ContainerId
                    toSend = json.dumps(obj.__dict__)
                    print("sending", toSend)
                    self.DeployResponse.send(self.DeploymentResponseTopic, value = toSend)
                    return True
            return False
        except Exception as e: #Handle Deployment Post Errors
            #raise e
            print(e)
            #return False



class ZipHelper:
    @staticmethod
    def zipdir(path, ziph):
        # ziph is zipfile handle
        for root, dirs, files in os.walk(path):
            for file in files:
                ziph.write(os.path.join(root, file), 
                        os.path.relpath(os.path.join(root, file), 
                                        os.path.join(path, '..')))
        
        



        
monitor_thread("ServiceLCM")
ServiceLifeCycleManager = ServiceLCM()
ServiceLifeCycleManager.StartListening()
