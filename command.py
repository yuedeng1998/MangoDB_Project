import pymongo
from pymongo import MongoClient
import datetime
import sys
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

def ls(data_path):
    # input:str 
    # handle root special case
    if data_path == '/':
        # find all tuples, only project their ids
        res = root.find({}, {'_id':1})
        for cont in res:
            # this is the is_dict tuple, ignore
            if cont['_id'] == ' ':
                continue
            print(cont['_id'])
        return
    dirs = data_path.split('/')
    # the first sub folder name, used to find the record
    _id = dirs[1]
    subpath = dirs[2:]
    filter = {"_id": _id}
    res = root.find_one(filter)
    # if no such parent dict, Error
    if not res:
        raise ValueError('No such dict')
    # Go to the sub folder, if exist
    for sub in subpath:
        if sub not in res:
            raise ValueError('No such dict')
        res = res[sub]
    # if not file or folder inside, print nothing
    for cont in res.keys():
        if cont == '_id' or cont == 'is_dict':
            continue
        print(cont)
def cat_file(data_path, file_path):
    pass

def rm_file(file_path):
    pass

def upload(data_path, file_path, k):
    pass


if len(sys.argv) < 3:
    raise ValueError('Need valid data path!')


instruction = sys.argv[1]
data_path = sys.argv[2]
if instruction == 'put':
    file_src = sys.argv[3]
    k = int(sys.argv[4])
init()
if instruction == 'mkdir':
    mkdir(data_path)
elif instruction == 'ls':
    ls(data_path)
elif instruction == 'cat':
    print(cat_file(data_path, file_src))
elif instruction == 'rm':
    rm_file(data_path)
    # data folder need at least 1 attribute or it will be delete,
    #  if data we want to delete already deleted, no error
elif instruction == 'put':
    upload(data_path, file_src, k)

