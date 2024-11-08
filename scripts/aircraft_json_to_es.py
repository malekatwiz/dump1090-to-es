## Python function called by a bash script with a file path, index_name, and elasticsearch URL as arguments

from datetime import date
import datetime
import json
import sys
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

def index_aircraft_data(file_path, index_name, es_url, device_name, mappings_file):
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
    
    
    # Read the file and prepare data for bulk indexing
    # JSON file contains a field "aircraft": []
    # Reads each element in the array and indexes it to Elasticsearch
    def read_and_prepare_data(filepath, index_name):
        actions = []
        with open(filepath, 'r', encoding='utf-8') as file:
            data = json.load(file)
            if "aircraft" in data:
                timestamp = datetime.datetime.now().isoformat()
                for item in data["aircraft"]:
                    hex = item.get("hex")
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

                    actions.append({
                        "_index": index_name,
                        "_source": {
                            "hex": hex,
                            "flight": flight,
                            "alt_baro": alt_baro,
                            "alt_geom": alt_geom,
                            "gs": gs,
                            "track": track,
                            "baro_rate": baro_rate,
                            "squawk": squawk,
                            "emergency": emergency,
                            "category": category,
                            "timestamp": timestamp,
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
                    })
    
        return actions
    
    # Load data and index to Elasticsearch
    actions = read_and_prepare_data(file_path, index_name)
    
    try:
        bulk(es, actions)
    except Exception as e:
        print(f"An error occurred while indexing data: {e}")
        sys.exit(1)
    print("Indexing complete.")

if __name__ == "__main__":
    if len(sys.argv) != 6:
        print("Usage: python aircraft_json_to_es.py <file_path> <index_name> <es_url> <mappings_file> <device_name>")
        sys.exit(1)
    else:
        file_path = sys.argv[1]
        index_name = sys.argv[2]
        es_url = sys.argv[3]
        mappings_file = sys.argv[4]
        device_name = sys.argv[5]
        index_aircraft_data(file_path, index_name, es_url, device_name, mappings_file)
