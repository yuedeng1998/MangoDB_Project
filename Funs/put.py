from warnings import filters
import pymongo
from pymongo import MongoClient
import datetime
import csv

client = pymongo.MongoClient(
    "mongodb+srv://yue:champion328@cluster0.lncwaco.mongodb.net/?retryWrites=true&w=majority")
db = client.hdfs
root = db.root
data = db.data

file_src = 'car.txt'
data_path = '/'
k = 5


def csv2json(csv_path, pk):
    if not pk:
        raise ValueError('Dont know partition on which key')
    out = {}
    with open(csv_path, encoding='utf-8') as data:
        data_rows = csv.DictReader(data)
        for row in data_rows:
            id = row[pk]
            out[id] = row
    return out


def partition_data(file_src, k, pk=""):
    _, ext = file_src.split('.')
    partitions = []
    if ext == 'csv':
        contents = csv2json(file_src, pk)
        maxkey = max(contents.keys())
        interval = int(maxkey)//k
        last = 0
        partitions = [[] for i in range(k)]
        for i in range(k):
            for key, row in contents.items():
                key = int(key)
                if i < k-1 and key >= i*interval and key < i*interval+interval:
                    partitions[i].append(row.copy())
                if i == k-1 and key >= (k-1)*interval:
                    partitions[i].append(row.copy())
        return partitions
    with open(file_src) as f:
        contents = f.read()
    if len(contents) < k:
        raise ValueError('Not enougth data to partition')
    partlen = int(len(contents)/k)
    last = 0
    for i in range(k-1):
        partitions.append(contents[i*partlen:i*partlen+partlen])
        last = i*partlen+partlen
    partitions.append(contents[last:])
    return partitions


def put(data_path, file_src):
    # get k partition address(a list of '_id' from data collection)
    data_ids = []
    for i in range(k):
        data_ids.append('p' + str(i))
    # write file content to those address
    data_partitions = partition_data(file_src, k, 'car_ID')
    for i, data_id in enumerate(data_ids):
        filter = {"_id": data_id}
        newvalues = {"$set": {'content': data_partitions[i]}}
        # if not exist in data, insert with data_id and content
        # else: overwrite
        if not data.find_one(filter):
            data.insert_one({'_id': data_id, 'content': data_partitions[i]})
        else:
            data.update_one(filter, newvalues)

    # update inside the meta root collection
    # 'file.ext' :{'p1': 'address1', 'p2':'address2'...}
    dirs = data_path.split('/')
    # the _id in mongodb: cannot be repeat! same as file system
    #       is the name of the first folder inside root
    _id = dirs[1]
    # the rest of the data_path in the format of 'dsd.new_dict'
    subpath = ".".join(dirs[2:])
    # print(subpath)
    filename = file_src.split('/')[-1]
    # {'p1':'address1'},{'p2': 'a2'}..
    file_content = {}
    for i, data_id in enumerate(data_ids):
        file_content['p'+str(i)] = str(data_id)
    # handle special cases; when add file from root
    if not subpath:
        # if store on root, insert hello.txt as _id and 'p1':'a1'...
        file_content = {'_id': filename}
        for i, data_id in enumerate(data_ids):
            file_content['p'+str(i)] = str(data_id)
        root.insert_one(file_content)

    else:
        subpath += '.' + filename

        filter = {"_id": _id}

    # the content under the file object {'wold.txt': {'p1':'address1'},{'p2': 'a2'}... }
        newvalues = {"$set": {subpath: file_content}}

    # if the sub root folder not exist, insert a new one
        if not root.find_one(filter):
            root.insert_one({'_id': _id, 'is_dict': True})
        # update the value to add the dict
        root.update_one(filter, newvalues)


put(data_path, file_src)
