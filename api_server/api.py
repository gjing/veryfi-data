from flask import Flask
from bson.json_util import dumps
from flask_pymongo import MongoClient
import os

app = Flask(__name__)
mongo_uri = os.environ.get("MONGODB_URL", False)


@app.route("/")
def hello_world():
    return "Server Running"


@app.route("/db/", methods=['GET'])
@app.route("/db/<string:oid>", methods=['GET'])
def get_data(oid=""):
    if not mongo_uri:
        return "DB Connection Error"
    client = MongoClient(mongo_uri)
    db = client['veryfi']
    collection = db['veryfi']
    print(client.server_info())
    if oid:
        return dumps(collection.find_one({"_id": oid}))
    return dumps(collection.find(batch_size=100))
