from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

# Initialize the Elasticsearch client
es = Elasticsearch("http://192.168.2.23:9200")  # Adjust URL if needed

# Define the index name
index_name = "airlines_records"

# File path to the .dat file
# Replace with downloaded file path (https://raw.githubusercontent.com/jpatokal/openflights/master/data/airlines.dat)
file_path = "C:\\temp\\airlines.dat"

# Ensure the index exists with the correct mapping (run this once)
if not es.indices.exists(index=index_name):
    es.indices.create(index=index_name, body={
        "mappings": {
            "properties": {
                "id": {"type": "keyword"},
                "name": {"type": "text"},
                "code1": {"type": "keyword"},
                "code2": {"type": "keyword"},
                "code3": {"type": "keyword"},
                "alias": {"type": "text"},
                "country": {"type": "text"},
                "active": {"type": "keyword"}
            }
        }
    })

# Read the file and prepare data for bulk indexing
def read_and_prepare_data(filepath):
    actions = []
    with open(filepath, 'r', encoding='utf-8') as file:
        for line in file:
            parts = line.strip().split(",")
            if len(parts) >= 8:  # Ensure there are enough columns
                actions.append({
                    "_index": index_name,
                    "_source": {
                        "airline_name": parts[1].strip('"'),
                        "airline_alias": parts[2].strip('"'),
                        "iata_code": parts[3].strip('"'),
                        "icao_code": parts[4].strip('"'),
                        "callsign": parts[5].strip('"'),
                        "country": parts[6].strip('"'),
                        "active": parts[7].strip('"')
                    }
                })
    return actions

# Load data and index to Elasticsearch
actions = read_and_prepare_data(file_path)
bulk(es, actions)

print("Indexing complete.")
