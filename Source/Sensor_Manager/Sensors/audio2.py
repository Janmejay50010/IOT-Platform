import socket
import string 
import random
import time
v=socket.socket()
port=8080
v.bind(("127.0.0.25",port))
v.listen(5)
conn,addr=v.accept()
while(1):
    print("waiting..audio")
    ran = ''.join(random.choices(string.ascii_uppercase + string.digits, k = 5))
    data=str(ran)   
    conn.send(data.encode())
    time.sleep(2)
conn.close()
v.close()
