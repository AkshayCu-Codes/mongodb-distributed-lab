from pymongo import MongoClient
from pprint import pprint

client = MongoClient("mongodb://172.18.0.4:27017,172.18.0.3:27018,172.18.0.2:27019/?replicaSet=rs0")

db = client['distributed_lab']
col = db['ConsistencyTest']

print("Documents in 'ConsistencyTest':")
for doc in col.find():
    pprint(doc)

client.close()
