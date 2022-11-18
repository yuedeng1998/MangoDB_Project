from warnings import filters
import pymongo
from pymongo import MongoClient
import datetime

client = pymongo.MongoClient(
    "mongodb+srv://yue:champion328@cluster0.lncwaco.mongodb.net/?retryWrites=true&w=majority")
db = client.hdfs
root = db.root
data = db.data

data_path = '/car.txt'


def rm(data_path):
    # Find the data address/_ids from the root collections
    dirs = data_path.split('/')
    # handle special cases; when rm file from root
    if len(dirs) == 2:
        # delete meta by deleting the record with name 'hello.txt'
        # to distinguish from folder named 'hello', only need on root
        _id = dirs[-1]
        filter = {"_id": _id}
        res = root.find_one(filter)
        print(res)
        dest_file_dict = res
        data_ids = []
        for key, cont in dest_file_dict.items():
            if key != '_id':
                data_ids.append(cont)
        root.delete_one(filter)
    else:

        _id = dirs[1]
        subpath = dirs[2:]
        # print(dirs, subpath)
        filename = subpath[-1]
        filename = filename.split('.')
        # print(filename)
        filter = {"_id": _id}
        res = root.find_one(filter)
        # if no such parent dict, Error
        if not res:
            raise ValueError('No such dict')
            # Go to the sub folder, if exist
        subpath = subpath[:-1]
        for sub in subpath:
            if sub not in res:
                # print(sub)
                raise ValueError('No such dict')
            res = res[sub]
        # inside the filename, can find the ext(eg. txt)
        dest_file_dict = res.copy()
        for file in filename:
            if file not in dest_file_dict:
                raise ValueError('No such file to rm')
            dest_file_dict = dest_file_dict[file]

        data_ids = []
        for cont in dest_file_dict.values():
            data_ids.append(cont)

        # delete inside the meta root collection

        extion = filename[1]
        # into the filename doc to check each extstion
        exts = list(res[filename[0]].keys())
        # print(exts)
        for ext in exts:
            if extion == ext:
                del res[filename[0]][ext]

        if not res[filename[0]]:
            del res[filename[0]]
        subpath = '.'.join(subpath)
        # print('the_path_is:', subpath, 'content', res)
        newvalues = {"$set": {subpath: res}}
        dest_filename = root.find_one(filter)
        root.update_one(filter, newvalues)

    # find each ids in data collections and rm it; free the ids
    # print(data_ids)
    for i, data_id in enumerate(data_ids):
        filter = {"_id": data_id}
        if not data.find_one(filter):
            raise ValueError('No content in data addrees')
        data.delete_one(filter)


rm(data_path)
# # Find the data address/_ids from the root collections
# dirs = data_path.split('/')
# _id = dirs[1]
# subpath = dirs[2:]
# filename = file_src.split('/')[-1]
# filename = filename.split('.')
# print(filename)
# # subpath += filename
# # subpath += filename
# # subpath += '.' + ext
# filter = {"_id": _id}
# res = root.find_one(filter)
#     # if no such parent dict, Error
# if not res:
#     raise ValueError('No such dict')
#     # Go to the sub folder, if exist
# for sub in subpath:
#     if sub not in res:
#         print(sub)
#         raise ValueError('No such dict')
#     res = res[sub]
# # inside the filename, can find the ext(eg. txt)
# dest_file_dict = res.copy()
# for file in filename:
#     if file not in dest_file_dict:
#         raise ValueError('No such file to rm')
#     dest_file_dict = dest_file_dict[file]

# data_ids = []
# for cont in dest_file_dict.values():
#     data_ids.append(cont)

# # delete inside the meta root collection

# extion = filename[1]
# # into the filename doc to check each extstion
# exts = list(res[filename[0]].keys())
# print(exts)
# for ext in exts:
#     if extion == ext:
#         del res[filename[0]][ext]

# if not res[filename[0]]:
#     del res[filename[0]]
# subpath = '.'.join(subpath)
# print('the_path_is:', subpath, 'content', res)
# newvalues = { "$set": {subpath:res}}
# dest_filename = root.find_one(filter)
# root.update_one(filter, newvalues)


# # find each ids in data collections and rm it; free the ids

# for i,data_id in enumerate(data_ids):
#     filter = {"_id": data_id}
#     if not data.find_one(filter):
#         raise ValueError('No content in data addrees')
#     data.delete_one(filter)
