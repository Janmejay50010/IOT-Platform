import time
from flask import *
import os
from file_validator import validate_app,validate_sensor_instance_config,validate_sensor_type_config
import db_handler
import monitoring_api

#import dummy_com as communication_api #### remove this and uncomment next line
import communication_api

import algo_instance_creator

app = Flask(__name__)

conf=dict()
with open("app_manager.config") as f:
    data=f.read().split('\n')
    for i in data:
        temp_conf=i.split(':')
        if len(temp_conf)>1:
            conf[temp_conf[0]]=temp_conf[1]

app_repo=conf['app_repo']
sensor_repo=conf['sensor_repo']
temp_files=conf['temp_files']

app.config['upload_extensions']=['zip','json']
app.config['upload_path']=temp_files
roles=["admin","developer","user"]

'''
    /sensor_registration, new_logs and logs are for admins pov
'''
@app.route("/sensor_registration",methods=["GET","POST"])
def sensor_registration():
    if 'user' not in session:
        return redirect("/")
    if request.method=="GET":
        return render_template("sensor_registration.html")
    if request.method=="POST":
        reg_type=request.form['reg_type']
        file_received=request.files['sensor_file']
        if not os.path.exists('temp'):
            os.mkdir('temp')
        new_file_name=str(round(time.time()))+".json"
        save_path=os.path.join(app.config['upload_path'],new_file_name)
        file_received.save(save_path)
        valid_file=False
        sen_list=None
        try:
            if reg_type=="type_registration":
                valid_file=validate_sensor_type_config(save_path,new_file_name)
                print("Check")
                print(valid_file)
            if reg_type=="instance_registration":
                #print(reg_type)
                valid_file,sen_list=validate_sensor_instance_config(save_path,new_file_name)
            if valid_file:
                with open(sensor_repo+new_file_name) as f:
                    json_info=json.load(f)
                    communication_api.appmanager_senmanager({"se_type":reg_type,"se_data":json_info})
                if reg_type=="instance_registration":
                    #print(sen_list)
                    db_handler.add_sensor(sen_list)
                flash("Registration successful")
        except:
            flash("Worng Format")
        if not valid_file:
            flash("Worng Format")
        return redirect("/sensor_registration")
    return

@app.route("/new_logs")
def new_logs():
    if 'user' not in session or session['role']!='admin':
        redirect('/')
    status=db_handler.get_component_status()
    # print(status)
    # print(type(status))
    return jsonify(log=status)

@app.route("/logs")
def logs():
    if 'user' not in session or session['role']!='admin':
        redirect('/')
    return render_template('logs.html')

"""
    /myapps , /view_app/<app_name> , /myalgo , /myapp/<app_name> are for users pov
"""

# list all applications and user's scheduled algos
@app.route("/myapps")
def my_apps():
    if 'user' not in session:
        redirect('/')
    all_apps=set(db_handler.get_all_applications())
    my_algos=db_handler.get_user_algos(session['user'])
    #print(my_algos)
    return render_template("myapps.html",allapps=all_apps,myalgos=my_algos)

# show app's algos to schedule an algo
@app.route("/view_app/<app_name>",methods=["GET","POST"])
def view_app_details(app_name):
    if 'user' not in session:
        return redirect("/")
    app=db_handler.get_application_for_user(app_name)
    user_algos=db_handler.get_user_algos(session['user'])
    all_sensors=db_handler.get_sensor_list()
    if request.method=="GET":
        return render_template("show_app_algo.html",apps=app["Algorithm"],algos=user_algos,all_sensors=all_sensors)
    
    if request.method=="POST":
        base_algo=request.form['base_algo']
        algo_id=request.form['algo_id']
        if algo_id in user_algos:
            flash("You already have algorithm with that name")
            return redirect("/view_app/"+app_name)
        run_type=request.form['run_type']
        base_algo_detils=None
        sensor_dict=dict()
        for algo in app["Algorithm"]:
            if algo["AlgorithmName"]==base_algo:
                base_algo_detils=algo
        for i in range(len(base_algo_detils['sensors'])):
            sensor_dict[base_algo_detils['sensors'][i]]=list()
            if base_algo_detils['sensor_num'][i]=="all":
                sensor_dict[base_algo_detils['sensors'][i]].append("all")
            else:
                for j in range(base_algo_detils['sensor_num'][i]):
                    sensor_dict[base_algo_detils['sensors'][i]].append(request.form[base_algo_detils['sensors'][i]+str(j)])
        
        action_dict=None
        if 'action' in base_algo_detils:
            action_dict=dict()
            for action in base_algo_detils['action']:
                if len(action)==0: 
                    continue
                if action=='callback':
                    action_dict['callback']=request.form[action]
                    if 'callback_email' in request.form:
                        action_dict['email']=request.form['callback_email']
                    if 'callback_sms' in request.form:
                        action_dict['sms']=request.form['callback_sms']
                    pass
                else:
                    action_dict[action]=request.form[action]
        #print(action_dict)

        if run_type=="now":
            interval=request.form['interval']
            db_handler.add_user_algo_instance(session['user'],base_algo,algo_id,run_type,sensor_dict,action_dict,interval=interval)
            zip_path=algo_instance_creator.create_algo_in_repo(session['user'],algo_id,base_algo,app_name,action_dict,sensor_dict)
            communication_api.appmanager_scheduler({'user':session['user'],'algorithmid':algo_id})
        else:
            schedule=dict()
            schedule['sched_type']=request.form['schedule_type']
            schedule['start_time']=request.form['start_time']
            schedule['end_time']=request.form['end_time']
            if schedule['sched_type']=="interval":
                schedule['interval']=request.form['interval']
            zip_path=algo_instance_creator.create_algo_in_repo(session['user'],algo_id,base_algo,app_name,action_dict,sensor_dict)
            db_handler.add_user_algo_instance(session['user'],base_algo,algo_id,run_type,sensor_dict,action_dict,schedule)

            communication_api.appmanager_scheduler({'user':session['user'],'algorithmid':algo_id})
        return redirect("/myapps")
    return


@app.route("/myalgo/<algo_id>",methods=["GET","POST"])
def show_algo(algo_id):
    if 'user' not in session:
        return redirect("/")
    algo_details=db_handler.get_algo(session['user'],algo_id)
    algo_status=db_handler.get_algo_status(session['user'],algo_id)
    print(algo_status)
    if request.method=="GET":
        return render_template("algo.html",user=session['user'],algo_details=algo_details,algo_status=algo_status)
    if request.method=="POST":
        get_request=request.form['switch'].lower()
        communication_api.appmanager_scheduler({'req_type':get_request,'user':session['user'],'algo_name':algo_details['AlgorithmName'],'algo_id':algo_id})
        return redirect("/myapps")


@app.route("/algo",methods=['GET'])
def get_algo_output():
    if 'user' not in session:
        return redirect('/')
    algoID=request.args['algoID']
    algo_name=db_handler.get_algo(session['user'],algoID)
    algo_name=algo_name['AlgorithmName']
    communication_api.application_deployer({'User':session['user'],'AlgorithmID':algoID,'AlgorithmName':algo_name})
    app_log=communication_api.deployer_application()
    if app_log['Status']=="OK":
        return app_log["Message"].replace('\n','<br>')
    else:
        return "Error in getting info"


@app.route("/algo_output/<user>/<algoID>")
def show_algo_output(user,algoID):
    if 'user' not in session or session['user']!=user:
        return redirect('/')
    
    return render_template("algo_output.html",algo=algoID)


'''
   /apps and /app/<app_name> are for developers pov
'''

@app.route("/apps",methods=["GET","POST"])
def apps():
    if 'user' not in session:
        return redirect("/")
    apps=db_handler.get_applications(session['user'])
    if request.method=="GET":
        return render_template('apps.html',apps=apps)

    if request.method=="POST":
        app_name=request.form.get('app_name')
        #app_name=request.args['app_name']
        if db_handler.check_application_name(app_name):
            flash("Application Name exists")
        else:
            db_handler.add_application(session['user'],app_name)
            flash("Application created")
            return redirect("/app/"+app_name)
    return redirect("/apps")

@app.route("/app/<app_name>",methods=["GET","POST"])
def app_display(app_name):
    if 'user' not in session:
        redirect('/')
    app_algos=db_handler.get_algorithms(session['user'],app_name)
    if request.method=="GET":
        return render_template('app_upload.html',apps=app_algos)
    if request.method=="POST":
        file_received=request.files['app_file']
        if not os.path.exists('temp'):
            os.mkdir('temp')
        save_path=os.path.join(app.config['upload_path'],file_received.filename)
        file_received.save(save_path)
        
        try:
            is_valid,ret_val,algos=validate_app(save_path,-1) 
            if not is_valid:
                print(is_valid)
                flash("Wrong File Format")
            else:
                flash("Application added successfully")
                db_handler.add_algorithms(session['user'],ret_val,algos)
        except:
            flash("Failed in adding to repo") 
       
        return redirect("/app/"+app_name)

'''
   for all users 
'''

@app.route("/",methods=["GET","POST"])
def index():
    if request.method=="POST":
        user = request.form['user']
        passwd = request.form['pass']
        role = request.form['role']
        if db_handler.login(user,passwd,role)==1:
            session['user']=user
            session['role']=role
            if role=="admin":
                return redirect(url_for('logs'))
            if role=="developer":
                return redirect(url_for('apps'))
            if role=="user":
                return redirect(url_for('my_apps'))
        else:
            flash("Check credentials")
            return redirect("/")
    return render_template("index.html")

@app.route("/signout",methods=["GET","POST"])
def logout():
    session.pop('user', None)
    session.pop('role',None)
    return redirect("/")

@app.route("/signup",methods=["GET","POST"])
def signup():
    if request.method=="POST":
        user = request.form['user']
        passwd = request.form['pass']
        role=request.form['role']
        if db_handler.register(user,passwd,role)==1:
            flash("Successfully registered")
            return redirect("/")
        flash("User exists")
    return render_template("signup.html")


if __name__=="__main__":
    app.secret_key = 'super secret key'
    monitoring_api.monitor_thread("application_manager")
    app.run(host="0.0.0.0",debug=True,port=conf['port'])
