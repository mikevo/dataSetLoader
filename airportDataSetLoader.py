#!/usr/bin/env python3

import datetime
import yaml
import csv
import urllib.request
from pymongo import MongoClient

config = yaml.safe_load(open('config.json'))

mongo = MongoClient(config['dbServer'])
collectionName = 'airports'
airportCollection = mongo[config['dbName']][collectionName]

airportUrl = 'https://raw.githubusercontent.com/datasets/airport-codes/master/data/airport-codes.csv'

airport_file = csv.DictReader(urllib.request.urlopen(airportUrl).read().decode('utf-8').splitlines())

for airport in airport_file:
	if airport['ident']:
		if not airport['type']:
			del airport['type']

		if not airport['name']:
			airport['name']

		if not airport['latitude_deg']:
			airport['latitude_deg']

		if not airport['longitude_deg']:
			airport['longitude_deg']

		if not airport['elevation_ft']:
			airport['elevation_ft']

		if not airport['continent']:
			airport['continent']

		if not airport['iso_country']:
			airport['iso_country']

		if not airport['iso_region']:
			airport['iso_region']

		if not airport['municipality']:
			airport['municipality']

		if not airport['gps_code']:
			airport['gps_code']

		if not airport['iata_code']:
			airport['iata_code']

		if not airport['local_code']:
			airport['local_code']

		airport['created'] = datetime.datetime.utcnow()
		airport['updated'] = datetime.datetime.utcnow()

		airportCollection.update_one(
			{"ident":airport['ident']},
			{ "$set":airport},
			True
		)
