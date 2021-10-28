import socket
import random
import time
v=socket.socket()
port=4026
v.bind(("127.0.0.30",port))
v.listen(5)
conn,addr=v.accept()
while(1):
    print("waiting...lux")
    beg=0
    end=1500
    data=str(random.randint(beg, end))
    conn.send(data.encode())
    time.sleep(2)
conn.close()
v.close()
