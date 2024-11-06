# How to use the scripts

## Seeding Data from (provided file) to Elasticsearch

```bash
python ./airlines_es_seeder.py
```

## Exporting data from Elasticsearch to CSV

```bash
python ./elasticsearch_index_export.py http://localhost:9200 aircraft_data aircraft_data.json
```
