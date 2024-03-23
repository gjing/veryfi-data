from flask import Flask
import json, bson, ijson
from flask_pymongo import MongoClient
import os

from pymongo.errors import DuplicateKeyError, OperationFailure
from bson.objectid import ObjectId
from bson.errors import InvalidId

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
        return collection.find_one({"_id": oid})
    return collection.find()
