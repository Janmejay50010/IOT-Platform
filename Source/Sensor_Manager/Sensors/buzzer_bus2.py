import socket
import string 
import random
import time
v=socket.socket()
port=4016
v.bind(("127.0.0.27",port))
v.listen(5)

print("waiting..switch")

# state = 1
while(1):
    conn,addr=v.accept()
    print("waiting..switch")
    received_data = conn.recv(4096).decode()
    #print("Switch on the buzzer")
    if(received_data == "1"):
        print("Switch on the buzzer")
    elif (received_data == "0"):
        print("Switch off the buzzer")
    
    conn.close()
v.close()
