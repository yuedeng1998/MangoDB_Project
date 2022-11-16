import pymongo
from pymongo import MongoClient
import datetime

client = pymongo.MongoClient("mongodb+srv://yue:champion328@cluster0.lncwaco.mongodb.net/?retryWrites=true&w=majority")
db = client.hdfs
root = db.root
data = db.data
def init():
    collist = db.list_collection_names()
    # check if root and data collections in the database
    # if not insert a tuple 
    # is_dict is not the _id, but this tuple will never be dropped 
    if "root" not in collist:
        root.insert_one({'_id': ' ','is_dict':True})
    if 'data' not in collist:
        data.insert_one({'_id': ' ', 'is_dict':True})

def mkdir(data_path):
    # input:str in format '/person/dsd/new_dict'
    dirs = data_path.split('/')
    # the _id in mongodb: cannot be repeat! same as file system
    #       is the name of the first folder inside root
    _id = dirs[1]
    # the rest of the data_path in the format of 'dsd.new_dict'
    subpath = ".".join(dirs[2:])
    filter = {"_id": _id}
    # set up the new dict value
    newvalues = { "$set": {subpath: {'is_dict':True}} }
    # if the sub root folder not exist, insert a new one 
    if not root.find_one(filter):
        root.insert_one({'_id':_id, 'is_dict':True})
    # update the value to add the dict
    root.update_one(filter, newvalues)
data_path = '/hakso/hkjs/dct'

init()
mkdir(data_path)

# data_path = '/hakso/pe/dict'
# dirs = data_path.split('/')
# print(dirs)
# _id = dirs[1]
# subpath = ".".join(dirs[2:])
# print(subpath)
# newvalues = { "$set": {subpath: {'is_dict':True}}}
# # root.insert_one({'_id':'person'})
# filter = {"_id": _id}
# if not root.find_one(filter):
#     root.insert_one({'_id':_id, 'is_dict':True})
# root.update_one(filter, newvalues)
# personDocument = {
#   "name": { "first": "John", "last": "Hua" },
#   "birth": datetime.datetime(1922, 9, 23),
#   "death": datetime.datetime(1954, 2, 7),
#   "contribs": [ "Muring machine", "Muring test", "Muringery" ],
#   "views": 1050000,
#   'person': {'is_dict':True}
# }
# # root.insert_one(personDocument)


# filter = {"person":{'$ne': None}}

