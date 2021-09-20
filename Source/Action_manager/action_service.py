from time import sleep
from json import dumps
# from kafka import KafkaProducer
# from kafka import KafkaConsumer
# import datetime,sys
import sys
import smtplib

# importing the client from the twilio
from twilio.rest import Client
# Your Account Sid and Auth Token from twilio account
account_sid = 'ACf807b4e0550999d81e0a423f31bd90fa'
auth_token = 'baedf1abf2153bd7727377de7f0ed1a0'



# sys.path.append('communication_module')
# import communication_api as ca

def send_email(message,receiver):
    print("----------------------------")
    print(message)
    print("-----------------------------")
    # message = "Hello World"
    # msg = message.decode()
    # print(msg)
    s = smtplib.SMTP('smtp.gmail.com',587)
    s.starttls()
    s.login('internals0929@gmail.com','Sachin@29')
    s.sendmail("internals0929@gmail.com",receiver,message)
    print("Mail sent")
    s.quit()

def send_sms(message,number):
    print(message)

    client = Client(account_sid, auth_token)
    # sending message
    message = client.messages.create(body=message, from_='+19727379099', to=number)
    # printing the sid after success
    print("SMS sent")
