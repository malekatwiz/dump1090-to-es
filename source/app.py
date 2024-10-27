# simple app runs as a service on Raspberry Pi
# Reads generated dump1090-fa data located /run/dump1090-fa/aircraft.json
# Dumps data to Elasticsearch database
# Uses instructions provided on this page [](https://github.com/flightaware/dump1090/blob/master/README-json.md#history_0json-history_1json--history_119json)

import json
import time
import logging
from elasticsearch import Elasticsearch
from elasticsearch import helpers as es_helpers

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Set up Elasticsearch
es = Elasticsearch("http://192.168.2.161:9200/")

index_name = "aircraft_data"
files_path = '/run/dump1090-fa/'
# files_path = 'C:\\temp\\'

def read_json_file(file_path):
    # read the data from the file
    with open(file_path) as f:
        data = json.load(f)
    return data

def read_aircraft_file(file_path):
    # read the data from the file
    with open(file_path, 'r') as f:
        data = read_json_file(file_path)
        aircrafts = data['aircraft']
    
    # map lon and lat to GeoJSON with type and coordinates
    timestamp = int(time.time())
    for aircraft in aircrafts:
        aircraft['created_at'] = timestamp
        if 'lon' in aircraft and 'lat' in aircraft:
            aircraft['location'] = {
                "type": "Point",
                "coordinates": [aircraft['lon'], aircraft['lat']]
            }
    return aircrafts

def read_history_files(files_path, history_files_count):
    # read the data from the history files
    history_files = []
    for i in range(history_files_count):
        file_path = files_path + 'history_' + str(i) + '.json'
        with open(file_path) as f:
            data = json.load(f)
            history_files.append(data)

    # sort history files by 'now' key
    history_files.sort(key=lambda x: x['now'])

    # extract 'aircraft[]' from each document
    aircrafts = []
    for doc in history_files:
        aircrafts.extend(doc['aircraft'])
    return aircrafts

def index_aircraft_data(aircraft_data):
    # map fields for elasticsearch
    # add _index key to each document
    # add _id key to each document
    actions = []
    for doc in aircraft_data:
        action = {
            "_index": index_name,
            "_source": doc
        }
        actions.append(action)

    # index to elasticsearch
    indexing_stat = es_helpers.bulk(es, actions, stats_only=True)
    return indexing_stat

# main
if __name__ == "__main__":
    # on startup, load receiver.json file
    # set interval per "refresh" key

    receiver_file_content = read_json_file(files_path + 'receiver.json')
    interval_sec = receiver_file_content["refresh"] / 1000

    # index history files to Elasticsearch
    history_files_count = receiver_file_content["history"]
    history_data = read_history_files(files_path, history_files_count)
    indexing_stat = index_aircraft_data(history_data)

    last_run = time.time()
    aircraft_file_path = files_path + 'aircraft.json'

    while True:
        # read the data from the file
        aircrafts_data = read_aircraft_file(aircraft_file_path)
        success_count, failed_count = index_aircraft_data(aircrafts_data)
        logger.info(f"Indexed {success_count} documents to Elasticsearch")
        logger.info(f"Failed to index {failed_count} documents to Elasticsearch")
        time.sleep(interval_sec)
    # Close the Elasticsearch connection
    es.close()

# End of file