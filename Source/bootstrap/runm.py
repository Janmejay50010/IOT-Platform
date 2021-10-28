from os import system
import json

# filename1="KafkaConsumer.py"
# filename2="KafkaProducer.py"

with open('config.json') as f:
	data = json.load(f)
services = data['service']
dependency = data['dependency']
print(services)
modules = list(services.keys())
print(modules)
print(dependency)


# load config.json
# dict = {"service":
# 	{"application_manager":"ec2-3-15-199-89.us-east-2.compute.amazonaws.com",
# 	"deployer": "ec2-3-15-199-89.us-east-2.compute.amazonaws.com",
# 	"schedular": "ec2-3-15-199-89.us-east-2.compute.amazonaws.com",
# 	"action_Manager":"ec2-18-116-200-217.us-east-2.compute.amazonaws.com",
# 	"monitoring":"ec2-18-116-200-217.us-east-2.compute.amazonaws.com",
# 	"sensor_Manager":"ec2-18-116-200-217.us-east-2.compute.amazonaws.com",
# 	"bootstrap":"ec2-18-116-200-217.us-east-2.compute.amazonaws.com",
# 	"dependency": ["Communication_Module", "Server_LifeCycle", "Service_LifeCycle", "Schedular"]
# 	}
# }

def fun(module_name,instance_ip,file_path,file_name):
	system("gnome-terminal -e 'bash -c \"python3 ssh.py %s %s %s %s\"'"%(module_name,instance_ip,file_path,file_name))

for service in services:
	if service=="monitoring":
		continue
		system("gnome-terminal -e 'bash -c \"python3 %s/%s  \"'"%(services[service]['path'],services[service]['file']))
	else:
		fun(service,services[service]['server'],services[service]['path'],services[service]['file'])

# system("gnome-terminal -e 'bash -c \"python3 ssh.py\"'")
#system("gnome-terminal -e 'bash -c \"ssh -i test_platform.pem ubuntu@ec2-3-15-199-89.us-east-2.compute.amazonaws.com \"'")
# "bootstrap":"ec2-18-116-200-217.us-east-2.compute.amazonaws.com"},

# stdin, stdout, stderr = ssh.exec_command("{p}python - <<EOF\n{s}\nEOF".format(p=remotepypath, s=mymodule))
# print("stderr: ", stderr.readlines())
# print("stdout: ", stdout.readlines())
