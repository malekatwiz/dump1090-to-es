# Simple Python script to export an Elasticsearch index to a file

import argparse
import json
import os
import sys

from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan

def export_index(es, index, output_file):
    with open(output_file, 'w') as f:
        for doc in scan(es, index=index):
            f.write(json.dumps(doc['_source']) + '\n')

def main():
    parser = argparse.ArgumentParser(description='Export an Elasticsearch index to a file')
    parser.add_argument('elastic_host', help='The hostname of the Elasticsearch server')
    parser.add_argument('index', help='The name of the Elasticsearch index to export')
    parser.add_argument('output_file', help='The file to write the exported data to')
    args = parser.parse_args()

    es = Elasticsearch(args.elastic_host)
    if not es.ping():
        print('Error: Elasticsearch is not reachable')
        sys.exit(1)

    export_index(es, args.index, args.output_file)

if __name__ == '__main__':
    main()