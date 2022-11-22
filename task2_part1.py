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
#     print(subpath)
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


def mapPartition_spotify(file_path, partition_number, user_input):
    data_addresses = getPartitionLocations(file_path)
    data_id = data_addresses[partition_number]
    filter = {"_id": data_id}
    partition = data.find_one(filter)['content']
    output = []
    for song in partition:
        if user_input.get('artist') in song['artist_names'] and \
        song['peak_rank'] == user_input.get('peak_rank') and \
        float(song['danceability']) >= float(user_input.get('dance_left')) and \
        float(song['danceability']) <= float(user_input.get('dance_right')) and \
        float(song['energy']) >= float(user_input.get('energy_left')) and \
        float(song['energy']) <= float(user_input.get('energy_right')) and \
        int(song['weeks_on_chart']) >= int(user_input.get('weeks_on_chart_left')) and \
        int(song['weeks_on_chart']) <= int(user_input.get('weeks_on_chart_right')):
            output.append(song['track_name'])
    return output

def Reduce_spotify(file_path):
    addresses = getPartitionLocations(file_path)
    songs = []
    for key in addresses.keys():
        temp = mapPartition_spotify(file_path, key, user_input)
        if len(temp) == 0:
            continue
        for song in temp:
            songs.append(song)
    if len(songs) == 0:
        print('No songs matching your criteria were found')
    return songs

data_path = '/spotify/documents/spotify_top_charts_22_edit.csv'
user_input = {'artist': 'Justin Bieber', 'peak_rank': '1', 'weeks_on_chart_left': '10', \
              'weeks_on_chart_right': '1000', 'dance_left': '0.1', 'dance_right': '1', \
              'energy_left': '0', 'energy_right': '1'}
partition_number = 'p0'
print(mapPartition_spotify(data_path, partition_number, user_input))
print(Reduce_spotify(data_path))


def mapPartition_tiktok(file_path, partition_number, user_input):
    data_addresses = getPartitionLocations(file_path)
    data_id = data_addresses[partition_number]
    filter = {"_id": data_id}
    partition = data.find_one(filter)['content']
    output = []
    for song in partition:
        if user_input.get('artist') in song['artist_name'] and \
        float(song['danceability']) >= float(user_input.get('dance_left')) and \
        float(song['danceability']) <= float(user_input.get('dance_right')) and \
        float(song['energy']) >= float(user_input.get('energy_left')) and \
        float(song['energy']) <= float(user_input.get('energy_right')):
            output.append(song['track_name'])
    return output

def Reduce_tiktok(file_path):
    addresses = getPartitionLocations(file_path)
    songs = []
    for key in addresses.keys():
        temp = mapPartition_tiktok(file_path, key, user_input)
        if len(temp) == 0:
            continue
        for song in temp:
            songs.append(song)
    if len(songs) == 0:
        print('No songs matching your criteria were found')
    return songs

data_path = '/tiktok/documents/TikTok_songs_2022_edit.csv'
user_input = {'artist': 'Harry Styles', 'dance_left': '0.1', 'dance_right': '1', \
              'energy_left': '0', 'energy_right': '1'}
partition_number = 'p0'
print(mapPartition_tiktok(data_path, partition_number, user_input))
print(Reduce_tiktok(data_path))