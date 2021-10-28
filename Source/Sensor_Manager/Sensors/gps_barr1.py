import socket
from random import randint
import time
v=socket.socket()
port=4003
v.bind(("127.0.0.30",port))
v.listen(5)
conn,addr=v.accept()
while(1):
    print("waiting..gps")
    x, y = 0,0
    data=str(x)+":"+str(y)
    print(data)
    conn.send(data.encode())
    time.sleep(2)
conn.close()
v.close()