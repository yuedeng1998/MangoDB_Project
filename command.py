import pymongo
from pymongo import MongoClient
import datetime
import sys
client = pymongo.MongoClient(
    "mongodb+srv://yue:champion328@cluster0.lncwaco.mongodb.net/?retryWrites=true&w=majority")
db = client.hdfs
root = db.root
data = db.data


def init():
    collist = db.list_collection_names()
    # check if root and data collections in the database
    # if not insert a tuple
    # is_dict is not the _id, but this tuple will never be dropped
    if "root" not in collist:
        root.insert_one({'_id': ' ', 'is_dict': True})
    if 'data' not in collist:
        data.insert_one({'_id': ' ', 'is_dict': True})


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
    newvalues = {"$set": {subpath: {'is_dict': True}}}
    # if the sub root folder not exist, insert a new one
    if not root.find_one(filter):
        root.insert_one({'_id': _id, 'is_dict': True})
    # update the value to add the dict
    root.update_one(filter, newvalues)


def ls(data_path):
    # input:str
    # handle root special case
    if data_path == '/':
        # find all tuples, only project their ids
        res = root.find({}, {'_id': 1})
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


def upload(data_path, file_src, k):
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
    filename = file_src.split('/')[-1]
    subpath += '.' + filename
    filter = {"_id": _id}
    # {'p1':'address1'},{'p2': 'a2'}..
    file_content = {}
    for i, data_id in enumerate(data_ids):
        file_content['p'+str(i)] = str(data_id)
    # the content under the file object {'wold.txt': {'p1':'address1'},{'p2': 'a2'}... }
    newvalues = {"$set": {subpath: file_content}}

    # if the sub root folder not exist, insert a new one
    if not root.find_one(filter):
        root.insert_one({'_id': _id, 'is_dict': True})
        # update the value to add the dict
    root.update_one(filter, newvalues)


def getPartitionLocations(data_path):
    # find the the location od the file in root
    # the first sub folder name, used to find the record
    dirs = data_path.split('/')
    _id = dirs[1]
    subpath = dirs[2:]
    # last_dict is hello.txt forat, since need hello['txt]
    last_dict = dirs[-1]
    last_dict = last_dict.split('.')
    subpath.pop(-1)
    subpath += last_dict[-2:]
    filter = {"_id": _id}
    res = root.find_one(filter)
    print(subpath)
    # if no such parent dict, Error
    if not res:
        raise ValueError('No such dict')
    # Go to the sub folder, if exist
    for sub in subpath:
        if sub not in res:
            print(sub)
            raise ValueError('No such dict')
        res = res[sub]
    # return a dict {'p1':'a1', ....}
    return res


def cat_file(data_path):
    # Get the p_n: address_n -> the ids in data collection with order
    data_addresses = getPartitionLocations(data_path)

    # read and combine all partitions from data collection
    file_content = ""
    print(data_addresses)
    for i in range(len(data_addresses)):
        data_id = data_addresses['p'+str(i)]
        filter = {"_id": data_id}
        # cobine each partation content
        file_content += data.find_one(filter)['content']
    print(file_content)


def rm_file(file_path):
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
    print(cat_file(data_path))
elif instruction == 'rm':
    rm_file(data_path)
    # data folder need at least 1 attribute or it will be delete,
    #  if data we want to delete already deleted, no error
elif instruction == 'put':
    upload(data_path, file_src, k)
