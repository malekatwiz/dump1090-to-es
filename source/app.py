# simple app runs as a service on Raspberry Pi
# Reads generated dump1090-fa data located /run/dump1090-fa/aircraft.json
# Dumps data to Elasticsearch database
# Uses instructions provided on this page [](https://github.com/flightaware/dump1090/blob/master/README-json.md#history_0json-history_1json--history_119json)

import json
from datetime import datetime
import os
import time
import logging
from elasticsearch import Elasticsearch
from elasticsearch import helpers as es_helpers
import requests

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Set up Elasticsearch
elasticsearch_url = os.getenv("ELASTICSEARCH_URL", "http://localhost:9200/")

# TODO: better handle es_client instance
es = Elasticsearch(elasticsearch_url)

check_interval_sec = os.getenv("CHECK_INTERVAL_SEC", 1)
index_name = os.getenv("ELASTICSEARCH_INDEX", "aircraft_data")
json_dump_files_path = os.getenv("JSON_DUMP_FILES_PATH", "/run/dump1090-fa/")
backfill_on_startup = os.getenv("BACKFILL_ON_STARTUP", "false")
aircraft_file_path = json_dump_files_path + 'aircraft.json'
hostname = os.getenv("HOSTNAME", "node_1")
dump_rest_api = os.getenv("DUMP_API_GET", "http://localhost:8080/data.json")
use_rest_api = os.getenv("USE_DUMP_REST_API", "false")

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
    # get UTC timestamp
    timestamp = datetime.now().timestamp()
    for aircraft in aircrafts:
        aircraft['created_on'] = timestamp
        aircraft['created_by'] = hostname
        if 'lon' in aircraft and 'lat' in aircraft:
            aircraft['location'] = {
                "type": "Point",
                "coordinates": [aircraft['lon'], aircraft['lat']]
            }
    return aircrafts

def read_aircrafts_rest_api():
    try:
        response = requests.get(dump_rest_api)
        response.raise_for_status()
        data = response.json()
        
        if data is None or len(data) == 0:
            logger.warning("No data returned from API")
            return []
        
        # map lon and lat to GeoJSON with type and coordinates
        # get UTC timestamp
        timestamp = datetime.now().timestamp()
        for aircraft in data:
            aircraft['created_on'] = timestamp
            aircraft['created_by'] = hostname
            if 'lon' in aircraft and 'lat' in aircraft:
                aircraft['location'] = {
                    "type": "Point",
                    "coordinates": [aircraft['lon'], aircraft['lat']]
                }
        return data
    except requests.RequestException as e:
        logger.error(f"Failed to fetch data from API: {e}")
        return []

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
    try:
        indexing_stat = es_helpers.bulk(es, actions)
    except Exception as e:
        logger.error(f"Failed to index data to Elasticsearch: {e}")
        return 0, len(aircraft_data)
    return indexing_stat

# main
if __name__ == "__main__":
    if not es.ping():
        logger.error("Cannot connect to Elasticsearch")
        exit(1)
        
    aircrafts_data = []
    while True:
        if use_rest_api == "true":
            aircrafts_data = read_aircrafts_rest_api()
        else:
            # read the data from the file
            aircrafts_data = read_aircraft_file(aircraft_file_path)
        if (len(aircrafts_data) > 0):
            success_count, failed_count = index_aircraft_data(aircrafts_data)
            if (failed_count > 0):
                logger.warning(f"Failed to index {failed_count} documents to Elasticsearch")
            else:
                logger.info(f"Indexed {success_count} documents to Elasticsearch")
        time.sleep(check_interval_sec)
    # Close the Elasticsearch connection
    es.close()

# End of file
