## Python function called by a bash script with a file path, index_name, and elasticsearch URL as arguments

from datetime import date
import datetime
import json
import sys
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

def index_aircraft_data(file_path, index_name, es_url, device_name):
    # Initialize the Elasticsearch client
    es = Elasticsearch(es_url)
    
    # Ensure the index exists with the correct mapping (run this once)
    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name, body={
            "mappings": {
                "properties": {
                    "hex": {"type": "keyword"},
                    "flight": {"type": "keyword"},
                    "alt_baro": {"type": "integer"},
                    "alt_geom": {"type": "integer"},
                    "gs": {"type": "integer"},
                    "track": {"type": "integer"},
                    "baro_rate": {"type": "integer"},
                    "squawk": {"type": "keyword"},
                    "emergency": {"type": "keyword"},
                    "category": {"type": "keyword"},
                    "timestamp": {"type": "date"},
                    "lat": {"type": "float"},
                    "lon": {"type": "float"},
                    "nic": {"type": "float"},
                    "rc": {"type": "float"},
                    "seen_pos": {"type": "float"},
                    "version": {"type": "integer"},
                    "nav_qnh": {"type": "float"},
                    "nav_altitude_mcp": {"type": "integer"},
                    "nav_heading": {"type": "float"},
                    "lat_lon": {"type": "geo_point"},
                    "nav_modes": {"type": "text"},
                    "rssi": {"type": "float"},
                    "messages": {"type": "integer"},
                    "sil": {"type": "integer"},
                    "sil_type": {"type": "text"},
                }
            }
        })
    
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
    if len(sys.argv) < 4:
        print("Usage: python aircraft_json_to_es.py <file_path> <index_name> <es_url>")
    else:
        file_path = sys.argv[1]
        index_name = sys.argv[2]
        es_url = sys.argv[3]
        if len(sys.argv) == 5:
            device_name = sys.argv[4]
        else:
            device_name = "Unknown"
        index_aircraft_data(file_path, index_name, es_url, device_name)
