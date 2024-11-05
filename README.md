# dump1090-to-es
is a lightweight Python app that reads data from dump1090 and sends it to Elasticsearch, that's it.

## Requirements
- dump1090-fa or dump1090-mutability running on the same machine
- Access to Elasticsearch

## Installation
- TODO

## Configuration
make a copy of `example.env` and name it `.env`, then fill in the required values.

## About the data
thanks to the comprehensive work [flightaware dump1090](https://github.com/flightaware/dump1090/blob/master/README-json.md) explaining the metadata.

## How I used it
I used it to collect data from dump1090 and visualize it on Kibana, I created a dashboard that shows the number of aircrafts in the air, the number of aircrafts per country, and the number of aircrafts per altitude.

### Transforming the data

### Enriching the data with Airlines information

Using Elasticsearch to enrich the data by matching the airline ICAO code, imported data from [OpenFlights](https://openflights.org/data.php#airline)
See script provided [airlines_es_seeder.py](scripts/airlines_es_seeder.py)


## thanks to creators and maintainers of
- [dump1090]()
- [dump1090-fa]()
- [dump1090-mutability]()
- [Elasticsearch]()
- [Elasticsearch Python Client]()

## License
Use it as you wish, no restrictions.
```