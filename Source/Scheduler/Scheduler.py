# Schedule Library imported
import schedule
import time
import json
import communication_api
import db_handler
import threading
import monitoring_api

# Functions setup

def toDeployer(User,AlgorithmName,AlgorithmID):
    communication_api.scheduler_deployer({'User':User,'AlgorithmName':AlgorithmName,'AlgorithmID':AlgorithmID})

def toDeployerAbort(User,AlgorithmName,AlgorithmID):
    communication_api.scheduler_deployer_abort({'User':User,'AlgorithmName':AlgorithmName,'AlgorithmID':AlgorithmID})

def toDeployer_once(User,AlgorithmName,AlgorithmID):
    communication_api.scheduler_deployer({'User':User,'AlgorithmName':AlgorithmName,'AlgorithmID':AlgorithmID})

def toDeployerAbort_once(User,AlgorithmName,AlgorithmID):
    communication_api.scheduler_deployer_abort({'User':User,'AlgorithmName':AlgorithmName,'AlgorithmID':AlgorithmID})

def once_start(start,User,AlgorithmName,AlgorithmID):
    schedule.every().day.at(start).do(toDeployer_once,User=User,AlgorithmName=AlgorithmName,AlgorithmID=AlgorithmID)

def once_end(end,User,AlgorithmName,AlgorithmID):
     schedule.every().day.at(end).do(toDeployerAbort_once,User=User,AlgorithmName=AlgorithmName,AlgorithmID=AlgorithmID)

def regular_start(start,User,AlgorithmName,AlgorithmID):
    schedule.every().day.at(start).do(toDeployer,User=User,AlgorithmName=AlgorithmName,AlgorithmID=AlgorithmID)

def regular_end(end,User,AlgorithmName,AlgorithmID):
     schedule.every().day.at(end).do(toDeployerAbort,User=User,AlgorithmName=AlgorithmName,AlgorithmID=AlgorithmID)

def interval_start(interval,User,AlgorithmName,AlgorithmID):
    schedule.every(int(interval)).minutes.do(toDeployer,User=User,AlgorithmName=AlgorithmName,AlgorithmID=AlgorithmID)

def start_exe():
    print("Scheduling Started.....") 
    print("Waiting for Application Manager..")   
    while True:
        schedule.run_pending()

def add_prior_schedules():
    all_stored_schedules=db_handler.get_all_schedules()
    for sch in all_stored_schedules:
        if sch['run_type']=='scheduled':
            if sch['schedule']['sched_type']=='once':
                once_start(sch['schedule']['start_time'],sch['email'],sch['AlgorithmName'],sch['AlgorithmID'])
                once_end(sch['schedule']['end_time'],sch['email'],sch['AlgorithmName'],sch['AlgorithmID'])
            if sch['schedule']['sched_type']=='regular':
                regular_start(sch['schedule']['start_time'],sch['email'],sch['AlgorithmName'],sch['AlgorithmID'])
                regular_end(sch['schedule']['end_time'],sch['email'],sch['AlgorithmName'],sch['AlgorithmID'])
            if sch['schedule']['sched_type']=='interval':
                interval_start(sch['schedule']['interval'],sch['email'],sch['AlgorithmName'],sch['AlgorithmID'])


if __name__=="__main__":
    add_prior_schedules()
    monitoring_api.monitor_thread("scheduler")
    myThread = threading.Thread(target=start_exe)
    myThread.daemon = True
    myThread.start()
    
    while True:
        message = communication_api.scheduler_appmanager()
        
        print(message)
        #code to stop algo explicitly
        if 'req_type' in message:
            print("check")
            if message['req_type']=='stop':
                toDeployerAbort(message['user'],message['algo_name'],message['algo_id'])
                continue


        sch = db_handler.get_one_schedule(message['user'],message['algorithmid'])
        print("Application Scheduled to run - " ,message['algorithmid'])
        print(sch)
        if sch['run_type']=='scheduled':
            if sch['schedule']['sched_type']=='once':
                once_start(sch['schedule']['start_time'],sch['email'],sch['AlgorithmName'],sch['AlgorithmID'])
                once_end(sch['schedule']['end_time'],sch['email'],sch['AlgorithmName'],sch['AlgorithmID'])
            if sch['schedule']['sched_type']=='regular':
                regular_start(sch['schedule']['start_time'],sch['email'],sch['AlgorithmName'],sch['AlgorithmID'])
                regular_end(sch['schedule']['end_time'],sch['email'],sch['AlgorithmName'],sch['AlgorithmID'])
            if sch['schedule']['sched_type']=='interval':
                interval_start(sch['schedule']['interval'],sch['email'],sch['AlgorithmName'],sch['AlgorithmID'])
        else:
            if sch['interval']=='0':
                toDeployer(sch['email'],sch['AlgorithmName'],sch['AlgorithmID'])
            else:
                interval_start(sch['interval'],sch['email'],sch['AlgorithmName'],sch['AlgorithmID'])
        
    
