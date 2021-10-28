import socket
from random import randint
import time
v=socket.socket()
port=4007
v.bind(("127.0.0.30",port))
v.listen(5)
conn,addr=v.accept()
while(1):
    print("waiting..biometric")
    x=randint(0, 100)
    data="p"+str(x)
    #print(data)
    conn.send(data.encode())
    time.sleep(10)
conn.close()
v.close()