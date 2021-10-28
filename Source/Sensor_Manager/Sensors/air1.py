import socket
import random
import time
v=socket.socket()
port=8082
v.bind(("127.0.0.24",port))
v.listen(5)
conn,addr=v.accept()
while(1):
    print("waiting..air")
    beg=0
    end=500
    data=str(random.randint(beg, end))
    conn.send(data.encode())
    time.sleep(2)
conn.close()
v.close()
