## Python function called by a bash script with a file path, index_name, and elasticsearch URL as arguments

import datetime
import json
import logging
import sys

import requests
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

logger = logging.getLogger(__name__)
logger.setLevel(level="DEBUG")

def index_aircraft_data(aircraft_url):
    # Initialize the Elasticsearch client
    es = Elasticsearch(es_url)

    # load elasticsearch index mappings from file in JSON format
    with open(mappings_file, 'r', encoding='utf-8') as mapping_file:
        index_mapping = json.load(mapping_file)

    # Ensure the index exists with the correct mapping (run this once)
    index_exists = es.indices.exists(index=index_name)
    if index_exists:
        current_mapping = es.indices.get_mapping(index=index_name)
        if current_mapping[index_name]['mappings'] != index_mapping['mappings']:
            es.indices.put_mapping(index=index_name, body=index_mapping['mappings'])
    else:
        es.indices.create(index=index_name, body=index_mapping)
    
    bulk_actions = []
    mapped_aircraft = get_file_aircraft(aircraft_url, device_name)
    if mapped_aircraft:
        for x in mapped_aircraft:
            x_actions = aircraft_to_action(x)
            if x_actions:
                bulk_actions.append(x_actions)

    if len(bulk_actions) == 0:
        logger.warning("No aircraft data mapped")
        return
    
    try:
        bulk(es, bulk_actions)
    except Exception as e:
        logger.error(f"error while indexing data: {e}")
        sys.exit(1)
    logger.info(f"Indexed {len(bulk_actions)} item/s successfully")

def aircraft_to_action(item):
    if not item or item.get("hex") is None:
        return None

    aircraft_hex = item.get("hex")
    flight = item.get("flight").strip() if item.get("flight") else ""
    alt_baro = item.get("alt_baro") if item.get("alt_baro") else 0
    alt_geom = item.get("alt_geom") if item.get("alt_geom") else 0
    gs = item.get("gs") if item.get("gs") else 0
    track = item.get("track") if item.get("track") else 0
    baro_rate = item.get("baro_rate") if item.get("baro_rate") else 0
    squawk = item.get("squawk").strip() if item.get("squawk") else ""
    emergency = item.get("emergency").strip() if item.get("emergency") else ""
    category = item.get("category").strip() if item.get("category") else ""
    lat = item.get("lat") if item.get("lat") else 0.0
    lon = item.get("lon") if item.get("lon") else 0.0
    nic = item.get("nic") if item.get("nic") else 0.0
    rc = item.get("rc") if item.get("rc") else 0.0
    seen_pos = item.get("seen_pos") if item.get("seen_pos") else 0.0
    version = item.get("version") if item.get("version") else 0
    nav_qnh = item.get("nav_qnh") if item.get("nav_qnh") else 0.0
    nav_altitude_mcp = item.get("nav_altitude_mcp") if item.get("nav_altitude_mcp") else 0
    nav_heading = item.get("nav_heading") if item.get("nav_heading") else 0.0
    lat_lon = {
        "lat": lat,
        "lon": lon
    }
    nav_modes = item.get("nav_modes") if item.get("nav_modes") else []
    rssi = item.get("rssi") if item.get("rssi") else 0.0
    messages = item.get("messages") if item.get("messages") else 0
    sil = item.get("sil") if item.get("sil") else 0
    sil_type = item.get("sil_type").strip() if item.get("sil_type") else ""

    return {
        "_index": index_name,
        "_source": {
            "hex": aircraft_hex,
            "flight": flight,
            "alt_baro": alt_baro,
            "alt_geom": alt_geom,
            "gs": gs,
            "track": track,
            "baro_rate": baro_rate,
            "squawk": squawk,
            "emergency": emergency,
            "category": category,
            "timestamp": datetime.datetime.now().isoformat(),
            "lat": lat,
            "lon": lon,
            "nic": nic,
            "rc": rc,
            "seen_pos": seen_pos,
            "version": version,
            "nav_qnh": nav_qnh,
            "nav_altitude_mcp": nav_altitude_mcp,
            "nav_heading": nav_heading,
            "lat_lon": lat_lon,
            "nav_modes": nav_modes,
            "rssi": rssi,
            "messages": messages,
            "sil": sil,
            "sil_type": sil_type,
            "captured_by": device_name
        }
    }

def get_file_aircraft(path, device_id):
    try:
        with open(path, 'r', encoding='utf-8') as data_file:
            aircraft_data = json.load(data_file)

        if not aircraft_data:
            logger.warning("No data found in file")
            return []

        mapped_aircraft = []
        aircraft = aircraft_data["aircraft"]
        if aircraft:
            timestamp = datetime.datetime.now().timestamp()
            for x in aircraft:
                x['created_on'] = timestamp
                x['created_by'] = device_id
                if 'lon' in x and 'lat' in x:
                    x['location'] = {
                        "type": "Point",
                        "coordinates": [x['lon'], x['lat']]
                    }
                mapped_aircraft.append(x)
        logger.debug(f"Found aircraft {len(mapped_aircraft)}")
        return mapped_aircraft
    except requests.RequestException as e:
        logger.error(f"Failed to fetch data from API: {e}")
        return []

def get_rest_api_aircraft(path, device_id):
    try:
        mapped_aircraft = []
        response = requests.get(path)
        response.raise_for_status()
        data = response.json()

        if not data:
            logger.warning("No data returned from API")
            return []

        aircraft = data["aircraft"]
        if aircraft:
            timestamp = datetime.datetime.now().timestamp()
            for x in aircraft:
                x['created_on'] = timestamp
                x['created_by'] = device_id
                if 'lon' in x and 'lat' in x:
                    x['location'] = {
                        "type": "Point",
                        "coordinates": [x['lon'], x['lat']]
                    }
                mapped_aircraft.append(x)
        logger.debug(f"Found aircraft {len(mapped_aircraft)}")
        return mapped_aircraft
    except requests.RequestException as e:
        logger.error(f"Failed to fetch data from API: {e}")
        return []

if __name__ == "__main__":
    if len(sys.argv) != 6:
        print("Usage: python aircraft_json_to_es.py <data_url> <index_name> <es_url> <mappings_file> <device_name>")
        sys.exit(1)
    else:
        data_url = sys.argv[1]
        index_name = sys.argv[2]
        es_url = sys.argv[3]
        mappings_file = sys.argv[4]
        device_name = sys.argv[5]
        index_aircraft_data(data_url)
