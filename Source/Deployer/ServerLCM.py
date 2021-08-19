import time, json, boto3, copy
import sys, paramiko
from datetime import datetime, timedelta
import json, pymongo, numpy as np
from bson.json_util import dumps
from monitoring_api import monitor_thread

#server_data_file_path = "./Server_data.json"

class Server:
    def __init__(self, Name, Status, IpAddr, InstanceId, ImageId, PlatformService, Service, Region, Metrics, GPU):
        self.Name = Name
        self.Status = Status
        self.IpAddr = IpAddr
        self.InstanceId = InstanceId
        self.ImageId = ImageId
        self.PlatformService = PlatformService
        self.Service = Service
        self.Region = Region
        self.Metrics = Metrics
        self.GPU = GPU
    
    def get_normal_client(self):
        client_object = boto3.client('ec2', region_name = 'us-east-1', 
                            aws_access_key_id = aws_access_key_id,
                            aws_secret_access_key = aws_secret_access_key)
        
        return client_object
    
    def get_resource_client(self):
        client_object = boto3.resource('ec2', region_name = 'us-east-1', 
                            aws_access_key_id = aws_access_key_id,
                            aws_secret_access_key = aws_secret_access_key)
        
        return client_object
    
    def get_cloudwatch_client(self):
        client_object = boto3.client('cloudwatch', region_name = 'us-east-1', 
                            aws_access_key_id = aws_access_key_id,
                            aws_secret_access_key = aws_secret_access_key)
        
        return client_object

    def start(self):
        ec2 = self.get_normal_client()
        try:
            response = ec2.start_instances(InstanceIds=[self.InstanceId], DryRun = False)
            print("Started {}".format(self.Name))
            #print(response)

            while(self.running_status() != "running"):
                self.insert_into_db()
                time.sleep(3)
                #server.start()
                continue
            
            print("{} is running now".format(self.Name))

            #update IP
            response = self.describe()
            self.IpAddr =  response['PublicIpAddress']

            if not self.PlatformService:
                #wait for some time
                time.sleep(20)
                print("SSHing into new server")
                try:
                    client = paramiko.SSHClient()
                    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                    key = paramiko.RSAKey.from_private_key_file("instance1.pem")

                    # Here 'ubuntu' is user name and 'instance_ip' is public IP of EC2
                    client.connect(hostname = self.IpAddr, username = "ubuntu", pkey = key)

                    commands = ["sudo systemctl start docker", "python3 -u /home/ubuntu/platform/Node.py 0.0.0.0 40000 10 > log.txt  2>&1 3>&1 4>&1 0>&1"]
                    
                    for cmd in commands:
                        print(cmd)
                        # Execute a command(cmd) after connecting/ssh to an instance
                        stdin, stdout, stderr = client.exec_command(cmd)
                        #ftp_client= client.open_sftp()
                        #ftp_client.put(localfilepath, remotefilepath)
                        #ftp_client.close()
                        time.sleep(5)
                        #print(stdout.read())

                    # close the client connection once the job is done
                    client.close()

                except Exception as e:
                    print("Error while Executing ssh in {} is {}".format(self.Name, e))
                    print(e)
        
        except Exception as e:
            print("Error when starting {} is {}".format(self.Name, e))
    
    def describe(self):
        try:
            ec2 = self.get_normal_client()
            response = ec2.describe_instances(InstanceIds = [self.InstanceId])
        
        except Exception as e:
            print("Error when describing {} is {} ".format(self.Name, e))
        
        response = response['Reservations'][0]['Instances'][0]
        return response

    def running_status(self):
        status = "None"
        try:
            response = self.describe()
            status = response['State']['Name']
            self.Status = status
        except Exception as e:
            print("Error when fetching running status for {} is {} ".format(self.Name, e))
        
        return status
    
    def get_stats(self, metric):
        
        client = self.get_cloudwatch_client()
        
        try:
            response = client.get_metric_statistics(
                Namespace='AWS/EC2',
                MetricName = metric,
                Dimensions=[
                    {
                    'Name': 'InstanceId',
                    'Value': self.InstanceId
                    },
                ],
                StartTime = datetime.now() - timedelta(hours = 1),
                EndTime = datetime.now(),
                Period = 60,
                Statistics = [
                    'Average',
                ],
                Unit = 'Percent'
            )
            #print(response)
            stat = round(np.random.uniform(low = .3, high = .8), 2)
            
            
            if len(response['Datapoints']) > 1000:
                latest_stat = response['Datapoints'][0]
                
                return latest_stat['Average']
            
            return stat
        
        except Exception as e:
            print("Error while fetching {} stats is {}".format(self.Name, e))
    
    def insert_into_db(self, key = "InstanceId"):
        uri = "mongodb+srv://janmejay:1234@cluster0.kpp8r.mongodb.net/myFirstDatabase?retryWrites=true"
        mongoclient = pymongo.MongoClient(uri)["IAS_Project"]["servers"]
        record = self.__dict__
        mongoclient.replace_one(
            {   key : record[key] },
                record,
                upsert = True
            )

class ServerLifecycle(Server):
    def __init__(self,aws_access_key_id, aws_secret_access_key, load_threshold, collection_client, launch_template_path):
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.load_threshold = load_threshold
        self.server_data = None
        self.collection_client = collection_client
        self.launch_template_path = launch_template_path
        self.servers = []

    def get_servers(self):
        # with open(server_data_file_path, "r") as file:
        #     servers_data = json.load(file)
        #     for server_name in servers_data.keys():
        #         server = Server(**servers_data[server_name])
        #         self.servers.append(server)
        cursor = self.collection_client.find(filter = {})
        for server_info in cursor:
            try:
                server_info.pop('_id')
                server = Server(**server_info)
                self.servers.append(server)
            
            except Exception as e:
                raise e
    
    def insert_into_db(self, record, key = "Name"):
        self.collection_client.replace_one(
            {   key : record[key] },
                record,
                upsert = True
            )

    def describe_new_server(self, instance_id):
        ec2 = self.get_normal_client()
        response = ec2.describe_instances(InstanceIds = [instance_id])
        response = response['Reservations'][0]['Instances'][0]

        return response

    def add_server(self, Gpu_server = False):
        try:
            with open(self.launch_template_path, 'r') as template_file:
                launch_template = json.load(template_file)
            
            if(Gpu_server):
                template_dict = launch_template["Gpu_server"]
            else:
                template_dict = launch_template["Normal_server"]
            
            ec2 = self.get_resource_client()
            response = ec2.create_instances(**template_dict)
            instance = response[0]
            InstanceId = instance.instance_id  
            
            status = self.describe_new_server(InstanceId)["State"]["Name"]
            while(status != "running"):
                status = self.describe_new_server(InstanceId)["State"]["Name"]
                continue
            
            response = self.describe_new_server(InstanceId)

            server_attributes = {}
            server_attributes["Name"] = "Server" + str(len(self.servers) + 1)
            server_attributes["Status"] = response["State"]["Name"]
            server_attributes["IpAddr"] =  response['PublicIpAddress']
            server_attributes["InstanceId"] = InstanceId
            server_attributes["ImageId"] = response["ImageId"]
            server_attributes["PlatformService"] = False
            server_attributes["Service"] =  "None"
            server_attributes["Region"] = "India"
            server_attributes["Metrics"] = {"CPUUtilization" : 0}
            server_attributes["GPU"] = False


            new_server = Server(**server_attributes)
            self.servers.append(new_server)

        except Exception as e:
            print("Error while adding new server is ", e)

    def check_load(self):
        num_loaded_servers = 0
        for server in self.servers:
            if server.Metrics["CPUUtilization"] > self.load_threshold or server.PlatformService:
                num_loaded_servers += 1
        
        num_servers = len(self.servers)
        if num_loaded_servers == num_servers:
        #if len(self.servers) < 4:
            pass
            #print("Adding a new server")
            #self.add_server()
        
        # elif  num_loaded_servers < num_servers - 3:
        #     pass
        #     remove unused server
            
    def get_status_and_stats(self):
        for server in self.servers:
            server.Status = server.running_status() 
            
            #check if server is running
            if server.Status == "disabled" or server.Status == "stopped":
                server.start()

            #get metrics from each server
            for metric_name in server.Metrics.keys():
                server.Metrics[metric_name] = server.get_stats(metric_name)
        
        #check if more servers need to be added
        self.check_load()
        
        #store info in database or json file
        print("Updating db")
        for server in self.servers:
            self.insert_into_db(server.__dict__)

        
def get_mongo_collection_client(uri, db = "IAS_Project", collection_name = "servers"):
    client = pymongo.MongoClient(uri)
    db = client[db]
    servers = db[collection_name]

    return servers

if __name__ == "__main__":
    mongodb = None
    aws_access_key_id = "AKIAZLNHU7MMUHAZ75DH"
    aws_secret_access_key = "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"
    #uri = "mongodb://janmejay:1234@cluster0-shard-00-00.kpp8r.mongodb.net:27017,cluster0-shard-00-01.kpp8r.mongodb.net:27017,   cluster0-shard-00-02.kpp8r.mongodb.net:27017/myFirstDatabase?ssl=true&replicaSet=atlas-ium7kk-shard-0&authSource=admin&retryWrites=true"
    uri = "mongodb+srv://janmejay:1234@cluster0.kpp8r.mongodb.net/myFirstDatabase?retryWrites=true"
    mongo_collection_client = get_mongo_collection_client(uri)
    load_threshold = .9
    launch_template_path = "./launch_template.json"

    #send health status to Monitoring module
    monitor_thread("ServerLCM")
    
    #Instantiate the class
    ServerLifeCycle = ServerLifecycle(aws_access_key_id, aws_secret_access_key, load_threshold, mongo_collection_client, launch_template_path)

    print("Starting Server LCM..\n")

    Frequency = 10
    while(True):
        time.sleep(Frequency)

        #send health status to Monitoring module
        
        #get servers data from a json file or mongodb
        ServerLifeCycle.get_servers()
        
        #get health and other stats from servers
        ServerLifeCycle.get_status_and_stats()
