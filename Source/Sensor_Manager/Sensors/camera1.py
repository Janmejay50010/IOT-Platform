import socket
import random
import time
v=socket.socket()
port=8080
v.bind(("127.0.0.30",port))
v.listen(5)
conn,addr=v.accept()
while(1):
    print("waiting.. camera")
    beg=10
    end=100
    data=random.randint(beg, end)
    conn.send(str(data).encode())
    time.sleep(5)
conn.close()
v.close()

