# simple app runs as a service on Raspberry Pi
# Reads generated dump1090-fa data located /run/dump1090-fa/aircraft.json
# Dumps data to Elasticsearch database
# Uses instructions provided on this page [](https://github.com/flightaware/dump1090/blob/master/README-json.md#history_0json-history_1json--history_119json)

import json
import requests
import time
import os
import logging
from elasticsearch import Elasticsearch
from elasticsearch import helpers as es_helpers

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Set up Elasticsearch
es = Elasticsearch("http://localhost:9200/")

files_path = '/run/dump1090-fa/'

def read_json_file(file_path):
    # read the data from the file
    with open(file_path) as f:
        data = json.load(f)
    return data

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

def index_data(history_files_count):
    aircrafts_data = read_history_files(files_path, history_files_count)

    # map fields for elasticsearch
    # add _index key to each document
    # add _id key to each document
    actions = []
    for doc in aircrafts_data:
        action = {
            "_index": "aircraft",
            "_source": doc
        }
        actions.append(action)

    # index to elasticsearch
    indexing_stat = es_helpers.bulk(es, actions, stats_only=True)
    return indexing_stat

# main loop
if __name__ == "__main__":
    # on startup, load receiver.json file
    # set interval per "refresh" key

    receiver_file_content = read_json_file(files_path + 'receiver.json')

    interval_sec = receiver_file_content["refresh"] / 1000
    history_files_count = receiver_file_content["history"]

    last_run = time.time() - interval_sec
    while True:
        if (time.time() - last_run) > interval_sec:
            last_run = time.time()
            indexing_stat = index_data(history_files_count)
            logger.info("Indexed %d documents", indexing_stat['index'])
        else:
            time.sleep(interval_sec)

    # Close the Elasticsearch connection
    es.close()

# End of file