import docker
import threading
class Docker:
    def __init__(self, rootDirectory = None) -> None:
        self.path_to_app_repo = "./usersNode/"
        self.client = docker.from_env()
        self.onGoingDeployments = {}

    def Deploy(self,algoId, algoName, userName):
        app_path = self.path_to_app_repo + userName + "/" + algoId
        algo_fileName = algoName + ".py"
        self.CreateDockerFile(app_path, algo_fileName)
        image_tag = ''.join(e for e in "{}_{}".format(userName, algoId) if e.isalnum())
        image_name = self.create_image(app_path, image_tag)
        container = self.run_container(image_name, detach_mode = True)
        return container.id

    def CreateDockerFile(self, docker_file_path, algo_file, language = "python:3.7"):
        with open(docker_file_path + "/" + "Dockerfile", "w") as dockerfile:
            #base image
            dockerfile.write("FROM {} \n".format(language))
            
            #create directory
            dockerfile.write("RUN mkdir /app \n")
            
            #change working directory
            dockerfile.write("WORKDIR /app \n")

            #add all the executable files to working directory
            dockerfile.write("ADD . /app/ \n")

            #command to install requirments
            dockerfile.write("RUN pip3 install -r requirements.txt \n")
            dockerfile.write("CMD [\"python3\", \"-u\" ,\""+algo_file+"\"]")

    def create_image(self,docker_file_path, image_name, custom_tag = ":latest"):
        #create docker client
        #tag is actually the name of the image
        Img_obj, json_obj = self.client.images.build(path = docker_file_path, tag = image_name + custom_tag)
        #print("Img object is", Img_obj)
        #print("Json object is", json_obj)
        print("Image built with the name => {}".format(image_name))

        return image_name
    
    def run_container(self,image_name, detach_mode = False, custom_tag = ":latest"):
        #create docker client
        client = self.client
        
        #run container
        if(detach_mode):
            container = client.containers.run(image_name + custom_tag, detach = detach_mode, network_mode = "host")
            print("Container created in detached mode")
            #time.sleep(2)
            #logs = container.logs(stream = False)
            #if we wait for streaming logs, then even in detached mode this will be effectively a blocking call
            #for line in logs:
            #    print(line)
            return container
        
        else:
            container = client.containers.run(image_name + custom_tag, detach = False, network_mode = "host")
            print("Container created in non-detached mode")
            print(container.logs())
            
            return container
    
    def PruneContainers(self):
        deletedNodes = self.client.containers.prune()
        return deletedNodes

    def GetFailedContainers(self):
        failedContainers = self.client.containers.list(all = True, filters={"exited" : 1})
        failedContainers.extend(self.client.containers.list(all = True, filters={ "exited" : 137}))
        return failedContainers
    
    def GetAllContainers(self):
        allContainers = self.client.containers.list(all = True)
        return allContainers
    
    def GetContainerById(self, containerId):
        container = self.client.containers.get(containerId)
        return container
        
