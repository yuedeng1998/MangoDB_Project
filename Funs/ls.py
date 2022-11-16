from warnings import filters
import pymongo
from pymongo import MongoClient
import datetime

client = pymongo.MongoClient("mongodb+srv://yue:champion328@cluster0.lncwaco.mongodb.net/?retryWrites=true&w=majority")
db = client.hdfs
root = db.root
data = db.data

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
# eg 1
data_path = '/'
print('ls', data_path)
ls(data_path)

# eg 2
data_path = '/person'
print('ls', data_path)
ls(data_path)

# eg 3
data_path = '/hakso/he'
print('ls', data_path)
ls(data_path)
# TODO: further test with txt, csv files


# # dirs = data_path.split('/')
# # _id = dirs[1]
# # subpath = dirs[2:]
# # print(subpath)
# # filter = {"_id": _id}
# # res = root.find_one(filter)
# res = root.find({}, {'_id':1})
# if not res:
#     raise ValueError('No such dict')
# # for sub in subpath:
# #     if sub not in res:
# #         raise ValueError('No such dict')
# #     res = res[sub]
# for cont in res:
#     if cont['_id'] == ' ':
#         continue
#     print(cont['_id'])


# dirs = data_path.split('/')
# _id = dirs[1]
# subpath = dirs[2:]
# filter = {"_id": _id}

# res = root.find_one(filter, {'_id':0, 'is_dict':0})
# for sub in subpath:
#     res = res[sub]
# print(res.keys())


