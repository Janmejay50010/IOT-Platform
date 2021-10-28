import socket
import string 
import random
import time
v=socket.socket()
port=8081
v.bind(("127.0.0.29",port))
v.listen(5)

print("waiting..switch")

# state = 1
while(1):
    conn,addr=v.accept()
    #print("waiting..switch")
    received_data = conn.recv(4096).decode()
    #print(received_data)
    if(received_data == "0"):
        print("Signal Green")
    elif(received_data == "1"):
        print('Signal Yellow')     
    else:
        print('Signal Red')
    #time.sleep(2)
    conn.close()
v.close()
