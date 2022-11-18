from warnings import filters
import pymongo
from pymongo import MongoClient
import datetime

client = pymongo.MongoClient(
    "mongodb+srv://yue:champion328@cluster0.lncwaco.mongodb.net/?retryWrites=true&w=majority")
db = client.hdfs
root = db.root
data = db.data
data_path = '/epople/pe/car.txt'


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


def cat(data_path):
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


cat(data_path)
