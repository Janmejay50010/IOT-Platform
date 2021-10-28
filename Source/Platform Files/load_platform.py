import os
import paramiko

instance_ip1 = "13.126.112.126"
instance_ip2 = "13.233.109.147"
path_to_key = "iotplatform.pem"
# modules_path = "/home/bugs/IAS/Platform/Platform_modules"
# instance_path = ['instance_ip1','instance_ip2']

def load_files(instance_ip,path):                     
	ssh = paramiko.SSHClient() 
	ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
	key = paramiko.RSAKey.from_private_key_file(path_to_key)
	ssh.connect(hostname=instance_ip, username="ubuntu", pkey=key)
	sftp = ssh.open_sftp()
	print("Hello")
	i=0
	for (root,dirs,files) in os.walk(path,topdown=True):
		print(root)
		print(dirs)
		print(files)
		if i!=0:
			sftp.mkdir(root) # platform ->
		i+=1 
		sftp.chdir(root)
		#for directory in dirs:
			#sftp.mkdir(directory)
		
		for f in files:
			sftp.put(root+"/"+f,f)
		sftp.chdir(None)
	sftp.close()
	ssh.close()

# choose one instance
load_files(instance_ip1,'platform')
#load_files(instance_ip2,'platform')
