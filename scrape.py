#!/usr/bin/env python

import sys
import time
import yaml
import logging

from scraper.Parser import Gesucht
from scraper.mongo_handler import Mongo

if __name__ == '__main__':

    with open(sys.argv[1]) as f:
        config = yaml.load(f)

    parser = Gesucht(config)
    mongo = Mongo(config['mongo'])

    listings = parser.get_listings(n=50)
    existing_listings = mongo.get_existing_urls(listings)
    new_listings = set(listings) - set(existing_listings)
    t0 =  time.time()
    for listing in new_listings:
        metrics = parser.parse_listing(listing)
        mongo.insert(metrics)
    logging.info('PARSE COMPLETED: {n} cases, {t} seconds'.format(n=len(new_listings), t=time.time()-t0))
