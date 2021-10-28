import socket
import string 
import random
import time
v=socket.socket()
port=4028
v.bind(("127.0.0.27",port))
v.listen(5)

print("waiting..switch")

# state = 1
while(1):
    conn,addr=v.accept()
    print("waiting..switch")
    received_data = conn.recv(4096).decode()
    #print("Switch on the ac of",received_data)
    if(received_data == "1"):
        print("Switch on")
    elif (received_data == "0"):
        print("Switch off")
    #time.sleep(2)
    
    conn.close()
v.close()
