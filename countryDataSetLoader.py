#!/usr/bin/env python3

import datetime
import json
import yaml
import urllib.request
from pymongo import MongoClient

config = yaml.safe_load(open('config.json'))

mongo = MongoClient(config['dbServer'])
collectionName = 'countries'
countryCollection = mongo[config['dbName']][collectionName]

countryUrl = 'https://raw.githubusercontent.com/mledoze/countries/master/dist/countries.json'
governmentUrl = 'https://raw.githubusercontent.com/samayo/country-data/master/src/country-government-type.json'
independenceUrl = 'https://raw.githubusercontent.com/samayo/country-data/master/src/country-independence-date.json'
expansionUrl = 'https://raw.githubusercontent.com/samayo/country-data/master/src/country-geo-cordinations.json'


country_file = urllib.request.urlopen(countryUrl)
independence_file =  yaml.safe_load(urllib.request.urlopen(independenceUrl))
gov_file = yaml.safe_load(urllib.request.urlopen(governmentUrl))
expansion_file = yaml.safe_load(urllib.request.urlopen(expansionUrl))

workAround = ''

for line in country_file:
	workAround += line.decode('UTF-8')

for country in json.loads(workAround):
	for gov in gov_file:
		if country['name']['common'] == gov['country']:
			country["government"] = gov['government']
			break

	for independence in independence_file:
		if country['name']['common'] == independence['country']:
			country["independence"] = independence['independence']
			break
	for expansion in expansion_file:
		if country['name']['common'] == expansion['country']:
			country['expanse'] = {}
			country['expanse']['north'] = expansion["north"]
			country['expanse']['south'] = expansion["south"]
			country['expanse']['east'] = expansion["east"]
			country['expanse']['west'] = expansion["west"]
			break

	country['created'] = datetime.datetime.utcnow()
	country['updated'] = datetime.datetime.utcnow()

	countryCollection.update_one(
		{"cca2":country['cca2']},
		{"$set":country},
		True
	)
