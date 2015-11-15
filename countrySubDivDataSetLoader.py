#!/usr/bin/env python3

import datetime
import yaml
import csv
import urllib.request
from pymongo import MongoClient

config = yaml.safe_load(open('config.json'))

mongo = MongoClient(config['dbServer'])
collectionName = 'countrySubDivs'
subDivCollection = mongo[config['dbName']][collectionName]

url = 'https://raw.githubusercontent.com/datasets/un-locode/master/data/subdivision-codes.csv'

subDiv_file = csv.DictReader(urllib.request.urlopen(url).read().decode('utf-8').splitlines())

count = 0
change = 0

for subDiv in subDiv_file:
	subDivDAO = {}
	count += 1
	if subDiv['SUCountry']:
		subDivDAO['country'] = subDiv['SUCountry']

		if subDiv['SUCode']:
			subDivDAO['division'] = subDiv['SUCode']
			change += 1

		if subDiv['SUName']:
			subDivDAO['name'] = subDiv['SUName']
			change += 1

		subDivDAO['created'] = datetime.datetime.utcnow()
		subDivDAO['updated'] = datetime.datetime.utcnow()

		subDivCollection.update_one(
			{"country":subDivDAO['country']},
			{ "$set":subDivDAO},
			True
		)
