#!/usr/bin/env python3

import datetime
import yaml
import csv
import urllib.request
from pymongo import MongoClient

config = yaml.safe_load(open('config.json'))

mongo = MongoClient(config['dbServer'])
collectionName = 'tlds'
tldCollection = countryCollection = mongo[config['dbName']][collectionName]

url = 'https://raw.githubusercontent.com/datasets/top-level-domain-names/master/top-level-domain-names.csv'

tld_file = csv.DictReader(urllib.request.urlopen(url).read().decode('utf-8').splitlines())

for tld in tld_file:
	tldDAO = {}
	if tld['Domain']:
		tldDAO['domain'] = tld['Domain']
		if tld['Type']:
			tldDAO['type'] = tld['Type']

		if tld['Sponsoring Organisation']:
			tldDAO['owner'] = tld['Sponsoring Organisation']

		tldDAO['created'] = datetime.datetime.utcnow()
		tldDAO['updated'] = datetime.datetime.utcnow()

		tldCollection.update_one(
			{"domain":tldDAO['domain']},
			{ "$set":tldDAO},
			True
		)
