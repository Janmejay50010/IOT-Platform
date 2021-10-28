import pymongo

client=pymongo.MongoClient("mongodb+srv://janmejay:1234@cluster0.kpp8r.mongodb.net/myFirstDatabase?retryWrites=true")

db = client['app_manager']

#pass component name and status
def update_component_status(component_name,status):
    db.component_status.update_one({"component_name":component_name},{"$set":{"status":status}},upsert=True)
    return None
