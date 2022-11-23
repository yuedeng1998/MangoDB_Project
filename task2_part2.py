import pymongo

client = pymongo.MongoClient(
    "mongodb+srv://yue:champion328@cluster0.lncwaco.mongodb.net/?retryWrites=true&w=majority")
db = client.hdfs
root = db.root
data = db.data

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

#return type改为list了
def cat_file(data_path):
    # Get the p_n: address_n -> the ids in data collection with order
    data_addresses = getPartitionLocations(data_path)

    # read and combine all partitions from data collection
    file_content = []
    for i in range(len(data_addresses)):
        data_id = data_addresses['p'+str(i)]
        filter = {"_id": data_id}
        # cobine each partation content
        file_content.extend(data.find_one(filter)['content']) 
    return file_content


def mapPartition_TikTok(file_path, partition_number):
    data_addresses = getPartitionLocations(file_path)
    data_id = data_addresses[partition_number]
    filter = {"_id": data_id}
    partition = data.find_one(filter)['content']

    title = []
    for item in partition:
        title.append(item['title'])
    
    tiktok = cat_file('/tiktok/documents/TikTok_songs_2022_edit.csv')
    tiktok_songs = []
    for song in tiktok:
        tiktok_songs.append(song['track_name'])
    
    rep = list(set(title).intersection(tiktok_songs))
    return rep

def Reduce_Tiktok(file_path):
    addresses = getPartitionLocations(file_path)
    rep = []
    for key in addresses.keys():
        new = mapPartition_TikTok(file_path, key)
        rep = list(set(rep + new))
    return len(rep)

def mapPartition_Spotify(file_path, partition_number):
    data_addresses = getPartitionLocations(file_path)
    data_id = data_addresses[partition_number]
    filter = {"_id": data_id}
    partition = data.find_one(filter)['content']
    
    title = []
    for item in partition:
        title.append(item['title'])
    
    spotify = cat_file('/spotify/documents/spotify_top_charts_22_edit.csv')
    spotify_songs = []
    for song in spotify:
        spotify_songs.append(song['track_name'])
    
    rep = list(set(title).intersection(spotify_songs))
    return rep

def Reduce_Spotify(file_path):
    addresses = getPartitionLocations(file_path)
    rep = []
    for key in addresses.keys():
        new = mapPartition_Spotify(file_path, key)
        rep = list(set(rep + new))
    return len(rep)

file_path = '/pastHotSongs/documents/pastHotSongs.csv'
partition_number = 'p5'
print(mapPartition_TikTok(file_path, partition_number))
print(Reduce_Tiktok(file_path))
partition_number = 'p25'
print(mapPartition_Spotify(file_path, partition_number))
print(Reduce_Spotify(file_path))
