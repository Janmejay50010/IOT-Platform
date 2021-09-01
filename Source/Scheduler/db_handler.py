from pymongo import MongoClient

#for online database
client = MongoClient("mongodb+srv://janmejay:1234@cluster0.kpp8r.mongodb.net/myFirstDatabase?retryWrites=true")

#for local mongodb
#client=MongoClient("0.0.0.0",27017)

db = client['app_manager']

#add admin if not already exists
if not db.users.find_one({"role":"admin"}):
    db.users.insert_one({"email":"admin@platform","password":"admin","role":"admin"})

#returns -1 if email already exist, 1 on successful registration
def register(email,password,role):
    existing_email=db.users.find_one({"email":email})
    if existing_email is not None:
        return -1
    db.users.insert_one({"email":email,"password":password,"role":role})
    return 1

#return -1 if email or password is wrong, 1 on success
def login(email,password,role):
    existing_email=db.users.find_one({"email":email,"password":password,"role":role})
    if existing_email is None:
        return -1
    return 1

#return list of algorithms, empty list in case of no algorithms
def get_algorithms(email,application):
    app=db.users.find_one({"email":email,"ApplicationName":application})
    if 'Algorithm' not in app:
        return list()
    else:
        return app['Algorithm']

#return -1 if no application is present, else list of applications
def get_applications(email):
    apps=db.users.find({"email":email,"ApplicationName":{"$exists":True}})
    if apps.count()==0:
        return -1
    else:
        return [app['ApplicationName'] for app in apps]

#return true if application name exists
def check_application_name(app_name):
    all_apps=db.users.find({"ApplicationName":{"$exists":True}})
    if all_apps.count()==0:
        False
    else:
        all_apps=[app['ApplicationName'] for app in all_apps]
        if app_name in all_apps:
            return True
        else:
            return False

#return 1, args: algorithms -> list of algorithms
def add_algorithms(email,application,algorithms):
    algo_list=[algo for algo in algorithms]
    db.users.find_one_and_update({"email":email,"ApplicationName":application,},{'$set': {"Algorithm":algo_list}})
    return 1

#return -1 if application name already exists, return 1 on successful addition
def add_application(email,application):
    apps=get_applications(email)
    if apps == -1 or application not in apps:
        db.users.insert_one({"email":email,"ApplicationName":application})
        return 1
    else:
        return -1

#add user algo instance
def add_user_algo_instance(email,base_algo,algo_instance,run_type,sensor_dict,action_dict,schedule=None):
    if run_type=="now":
        db.users.insert_one({"email":email,"AlgorithmName":base_algo,"AlgorithmID":algo_instance,"run_type":run_type,'sensor':sensor_dict,'action':action_dict})
    else:
        db.users.insert_one({"email":email,"AlgorithmName":base_algo,"AlgorithmID":algo_instance,"run_type":run_type,"schedule":schedule,'sensor':sensor_dict,'action':action_dict})
    return

#get schedule of specific application, return list of algo dictinaries 
def get_one_schedule(user,algoID):
    app_detail=db.users.find_one({'email':user,"AlgorithmID":algoID})
    return app_detail

#return all schedules
def get_all_schedules():
    all_apps=db.users.find({"AlgorithmID":{"$exists":True}})
    apps=list()
    for app in all_apps:
        if 'schedule' in app:
            apps.append(app)
    return apps

#return list
def get_all_applications():
    all_apps=db.users.find({"ApplicationName":{"$exists":True},"Algorithm":{"$exists":True}})
    if all_apps.count()==0:
        return list()
    else:
        all_apps=[app['ApplicationName'] for app in all_apps]
        return all_apps

#return list
def get_application_for_user(app_name):
    apps=db.users.find_one({"ApplicationName":app_name,"Algorithm":{"$exists":True}})
    return apps

#return list of user's algos(running or scheduled)
def get_user_algos(email):
    all_apps=db.users.find({"email":email,"AlgorithmName":{"$exists":True},"AlgorithmID":{"$exists":True}})
    if all_apps.count()==0:
        return list()
    else:
        all_apps=[app['AlgorithmID'] for app in all_apps]
        return all_apps

def get_algo(email,algo_id):
    algo_details=db.users.find_one({"email":email,"AlgorithmID":algo_id})
    return algo_details


def get_sensor_list():
    sensors=db.am_sensors.find({"sen_type":{"$exists":True}})
    if sensors.count()==0:
        return dict()
    else:
        sensor_dict=dict()
        for sensor in sensors:
            if sensor['sen_type'] not in sensor_dict:
                sensor_dict[sensor['sen_type']]=list()
            sensor_dict[sensor['sen_type']].append(sensor['sen_loc'])
        return sensor_dict

def add_sensor(sen_list):
    for i in sen_list:
        check=db.am_sensors.find(i)
        if check.count()==0:
            db.am_sensors.insert_one(i)
    return

def get_component_status():
    status=db.component_status.find({"component_name":{"$exists":True}})
    status_dict=dict()
    for i in status:
        status_dict[i['component_name']]=i['status']
    return status_dict

# print(register("abc@xyz.com","abc123"))
# print(login("abc@xyz.com","abc123"))
# print(get_applications("abc@xyz.com"))
# print(add_application("abc@xyz.com","app2"))
# print(get_applications("abc@xyz.com"))
# print(add_application("abc@xyz.com","app1"))
