import boto3
import paramiko
import sys
from time import sleep

def instance_handler(file_path,file_name,instance_ip):
	print("filepath:"+file_path+":"+file_name)
	# ls_cmd = 'cd '+file_path+';ls -la'
	cmd = 'cd '+file_path+';python3 -u '+file_name+" >> "+file_name+".txt"
	# instance_ip = "ec2-3-15-199-89.us-east-2.compute.amazonaws.com"
	path_to_key = "iotplatform.pem"

	client = paramiko.SSHClient()
	client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
	key = paramiko.RSAKey.from_private_key_file(path_to_key)

	# Connect/ssh to an instance
	try:
	    # Here 'ubuntu' is user name and 'instance_ip' is public IP of EC2
	    client.connect(hostname=instance_ip, username = "ubuntu", pkey = key)

	    stdin, stdout, stderr = client.exec_command(cmd)
	    print(stdout.read().decode())
	    print(stderr.read().decode())

	    # close the client connection once the job is done
	    client.close()

	except Exception as e:
	    print(e)

if __name__ == "__main__":
	n = len(sys.argv)
	print("Total arguments passed:",n) #module_name,instance_ip,path,file_name
	module_name = sys.argv[1]
	instance_ip = sys.argv[2]
	file_path = sys.argv[3]
	file_name = sys.argv[4]
	# file_path = 'platform/'+module_name+"/"+file_name
	print(file_path)
	print(file_name)
	print(instance_ip)
	instance_handler(file_path,file_name,instance_ip)
	# while(1):
	# 	print("Hello")
	# 	sleep(5)
	x = input()
	if(x==0):
		sys.exit()

# platform
# 	comp1
# 		files
# 	com2
# 		files
