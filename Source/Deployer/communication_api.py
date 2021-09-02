import prodapi
import json
import threading

kafkaIPPort = '52.15.89.83:9092'

def sensormanager_monitor(data):
    # prodapi.send_message('sen_mo',data)
    prodapi.heart_beat('sen_mo',data)

def monitor_sensormanager(lock,status,service_name,currtime):
    from kafka import KafkaConsumer
    consumer = KafkaConsumer('sen_mo',
    bootstrap_servers=[kafkaIPPort],
    # value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    consumer_timeout_ms=10000
    )
    for message in consumer:
        # th = threading.Thread(target=func,args=(message.value))
        # th.start()
        try:
            message = message.value.decode().split('_')
            last_active = message[0].strip('"')   # Json b kiya tha. chala pr thode issues the. to fir str hi rhne diya.. jyda kuch farak nhi pdega
            last_active = datetime.datetime.strptime(last_active, '%Y-%m-%d %H:%M:%S.%f')
            if(last_active>=currtime):                                          # Esa karne se purana status nhi print honga bar bar, sirf recent wala hi honga.
                print(message[1].strip('"'),datetime.datetime.now())
                if status[service_name] == 'down':
                    # print("Hello")
                    lock.acquire()
                    f1 = open("log_file.json",'w+')
                    status[service_name] = 'up'
                    log_json = json.dumps(status)
                    f1.write(log_json)
                    lock.release()
                    f1.close()

        except:
            1



def appmanager_monitor(data):
    prodapi.heart_beat('app_mo',data)

def monitor_appmanager(lock,status,service_name,currtime):
    from kafka import KafkaConsumer
    consumer = KafkaConsumer('app_mo',
    bootstrap_servers=[kafkaIPPort],
    # value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    consumer_timeout_ms=10000
    )
    for message in consumer:
        # th = threading.Thread(target=func,args=(message.value))
        # th.start()
        try:
            message = message.value.decode().split('_')
            last_active = message[0].strip('"')   # Json b kiya tha. chala pr thode issues the. to fir str hi rhne diya.. jyda kuch farak nhi pdega
            last_active = datetime.datetime.strptime(last_active, '%Y-%m-%d %H:%M:%S.%f')
            if(last_active>=currtime):                                          # Esa karne se purana status nhi print honga bar bar, sirf recent wala hi honga.
                print(message[1].strip('"'),datetime.datetime.now())
                if status[service_name] == 'down':
                    # print("Hello")
                    lock.acquire()
                    f1 = open("log_file.json",'w+')
                    status[service_name] = 'up'
                    log_json = json.dumps(status)
                    f1.write(log_json)
                    lock.release()
                    f1.close()

        except:
            1

def actionmanager_monitor(data):
    prodapi.heart_beat('act_mo', data)


def monitor_actionmanager(lock,status,service_name,currtime):
    from kafka import KafkaConsumer
    consumer = KafkaConsumer('act_mo',
    bootstrap_servers=[kafkaIPPort],
    # value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    consumer_timeout_ms=10000
    )
    for message in consumer:
        # th = threading.Thread(target=func,args=(message.value))
        # th.start()
        try:
            message = message.value.decode().split('_')
            last_active = message[0].strip('"')   # Json b kiya tha. chala pr thode issues the. to fir str hi rhne diya.. jyda kuch farak nhi pdega
            last_active = datetime.datetime.strptime(last_active, '%Y-%m-%d %H:%M:%S.%f')
            if(last_active>=currtime):                                          # Esa karne se purana status nhi print honga bar bar, sirf recent wala hi honga.
                print(message[1].strip('"'),datetime.datetime.now())
                if status[service_name] == 'down':
                    # print("Hello")
                    lock.acquire()
                    f1 = open("log_file.json",'w+')
                    status[service_name] = 'up'
                    log_json = json.dumps(status)
                    f1.write(log_json)
                    lock.release()
                    f1.close()

        except:
            1



def depmanager_monitor(data):
    prodapi.heart_beat('dep_mo',data)

def monitor_depmanager(lock,status,service_name,currtime):
    from kafka import KafkaConsumer
    consumer = KafkaConsumer('dep_mo',
    bootstrap_servers=[kafkaIPPort],
    # value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    consumer_timeout_ms=10000
    )
    for message in consumer:
        # th = threading.Thread(target=func,args=(message.value))
        # th.start()
        try:
            message = message.value.decode().split('_')
            # print(message)
            last_active = message[0].strip('"')   # Json b kiya tha. chala pr thode issues the. to fir str hi rhne diya.. jyda kuch farak nhi pdega
           
            last_active = datetime.datetime.strptime(last_active, '%Y-%m-%d %H:%M:%S.%f')
            
            if(last_active>=currtime):                                      # Esa karne se purana status nhi print honga bar bar, sirf recent wala hi honga.
                print(message[1].strip('"'),datetime.datetime.now())
                if status[service_name] == 'down':
                    # print("Hello")
                    lock.acquire()
                    f1 = open("log_file.json",'w+')
                    status[service_name] = 'up'
                    log_json = json.dumps(status)
                    f1.write(log_json)
                    lock.release()
                    f1.close()

        except:
            1

def scheduler_monitor(data):
    prodapi.heart_beat('sch_mo',data)

def monitor_scheduler(lock,status,service_name,currtime):
    from kafka import KafkaConsumer
    consumer = KafkaConsumer('sch_mo',
    bootstrap_servers=[kafkaIPPort],
    # value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    consumer_timeout_ms=10000
    )
    for message in consumer:
        # th = threading.Thread(target=func,args=(message.value))
        # th.start()
        try:
            message = message.value.decode().split('_')
            last_active = message[0].strip('"')   # Json b kiya tha. chala pr thode issues the. to fir str hi rhne diya.. jyda kuch farak nhi pdega
            last_active = datetime.datetime.strptime(last_active, '%Y-%m-%d %H:%M:%S.%f')
            if(last_active>=currtime):                                          # Esa karne se purana status nhi print honga bar bar, sirf recent wala hi honga.
                print(message[1].strip('"'),datetime.datetime.now())
                if status[service_name] == 'down':
                    # print("Hello")
                    lock.acquire()
                    f1 = open("log_file.json",'w+')
                    status[service_name] = 'up'
                    log_json = json.dumps(status)
                    f1.write(log_json)
                    lock.release()
                    f1.close()

        except:
            1

def depstack_monitor(data):
    prodapi.send_message('depst_mo',data)

def monitor_depstack(lock,status,service_name,currtime):
    from kafka import KafkaConsumer
    consumer = KafkaConsumer('depst_mo',
    bootstrap_servers=[kafkaIPPort],
    # value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    consumer_timeout_ms=10000
    )
    for message in consumer:
        # th = threading.Thread(target=func,args=(message.value))
        # th.start()
        try:
            message = message.value.decode().split('_')
            last_active = message[0].strip('"')   # Json b kiya tha. chala pr thode issues the. to fir str hi rhne diya.. jyda kuch farak nhi pdega
            last_active = datetime.datetime.strptime(last_active, '%Y-%m-%d %H:%M:%S.%f')
            if(last_active>=currtime):                                          # Esa karne se purana status nhi print honga bar bar, sirf recent wala hi honga.
                print(message[1].strip('"'),datetime.datetime.now())
                if status[service_name] == 'down':
                    # print("Hello")
                    lock.acquire()
                    f1 = open("log_file.json",'w+')
                    status[service_name] = 'up'
                    log_json = json.dumps(status)
                    f1.write(log_json)
                    lock.release()
                    f1.close()

        except:
            1

def topman_monitor(data):
   prodapi.heart_beat('top_mo',data)

def monitor_topman(lock,status,service_name,currtime):
    from kafka import KafkaConsumer
    consumer = KafkaConsumer('top_mo',
    bootstrap_servers=[kafkaIPPort],
    # value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    consumer_timeout_ms=10000
    )
    for message in consumer:
        th = threading.Thread(target=func,args=(message.value))
        th.start()

def topman_monitor_req(data):
    prodapi.send_message('top_mo_r',data)

def monitor_topman_req(lock,status,service_name,currtime):
    from kafka import KafkaConsumer
    consumer = KafkaConsumer('top_mo_r',
    bootstrap_servers=[kafkaIPPort],
    # value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    consumer_timeout_ms=10000
    )
    for message in consumer:
        th = threading.Thread(target=func,args=(message.value))
        th.start()
# Monitoring topics end here
def appmanager_depstack(data):
    prodapi.send_message('app_dep',data)

def depstack_appmanager(func):
    from kafka import KafkaConsumer
    consumer = KafkaConsumer('app_dep',
    bootstrap_servers=[kafkaIPPort],
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    for message in consumer:
        # th = threading.Thread(target=func,args=(message.value))
        return message.value
        # th.start()

def sensordata(topic):
    from kafka import KafkaConsumer
    consumer = KafkaConsumer(topic,
    bootstrap_servers=[kafkaIPPort],
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    for message in consumer:
        mess= message.value
        return mess
	

def depmanager_senmanager(data):
    prodapi.send_message('dep_sen',data)

def senmanager_depmanager(func):
    from kafka import KafkaConsumer
    consumer = KafkaConsumer('dep_sen',
    bootstrap_servers=[kafkaIPPort],
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    for message in consumer:
        th = threading.Thread(target=func,args=(message.value,))
        th.start()
        break

def depmanager_senmanager_rep(data):
    prodapi.send_message('dep_sen_rep',data)

def senmanager_depmanager_rep():
    from kafka import KafkaConsumer
    consumer = KafkaConsumer('dep_sen_rep',
    bootstrap_servers=[kafkaIPPort],
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    for message in consumer:
        return message.value 


def depmanager_appmanager(data):
    prodapi.send_message('dep_app',data)

def appmanager_depmanager(func):
    from kafka import KafkaConsumer
    consumer = KafkaConsumer('dep_app',
    bootstrap_servers=[kafkaIPPort],
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    for message in consumer:
        # return message
        th = threading.Thread(target=func,args=(message.value))
        th.start()

def depmanager_actmanager(data):
    prodapi.send_message('dep_act',data)

def actmanager_depmanager(func):
    from kafka import KafkaConsumer
    consumer = KafkaConsumer('dep_act',
    bootstrap_servers=[kafkaIPPort],
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    for message in consumer:
        # th = threading.Thread(target=func,args=(message.value))
        return message.value
        # th.start()


def appmanager_senmanager(data):
    prodapi.send_message('app_sen',data)

def senmanager_appmanager(func):
    from kafka import KafkaConsumer
    consumer = KafkaConsumer('app_sen',
    bootstrap_servers=[kafkaIPPort],
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    for message in consumer:
        # th = threading.Thread(target=func,args=(message.value))
        return message.value
        # th.start()
def appmanager_scheduler(data):
    prodapi.send_message('app_sch',data)


def scheduler_appmanager(func):
    from kafka import KafkaConsumer
    consumer = KafkaConsumer('app_sch',
    bootstrap_servers=[kafkaIPPort],
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    for message in consumer:
        th = threading.Thread(target=func,args=(message.value,))
        th.start()

def scheduler_deployer(data):
    prodapi.send_message('sch_dep',data)

def deployer_scheduler():
    from kafka import KafkaConsumer
    consumer = KafkaConsumer('sch_dep',
    bootstrap_servers=[kafkaIPPort],
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    for message in consumer:
        return message.value
        # th = threading.Thread(target=func,args=(message.value,))
        # th.start()
