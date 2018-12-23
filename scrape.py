#!/usr/bin/env python

import sys
import time
import yaml
import random
import logging

from .Parser import Gesucht
from .mongo_handler import Mongo

if __name__ == '__main__':

    with open(sys.argv[1]) as f:
        config = yaml.load(f)

    parser = Gesucht(config)
    mongo = Mongo(config['mongo'])

    listings = parser.get_listings(n=20)
    existing_listings = mongo.get_existing_urls(listings)
    new_listings = set(listings) - set(existing_listings)
    t0 =  time.time()

    for listing in new_listings:
        metrics = parser.parse_listing(listing)
        logging.info('{url} parsed'.format(url=listing))
        mongo.insert(metrics)
        logging.info('{url} inserted to postgreSQL'.format(url=listing))
        time.sleep(random.uniform(0, 3))
    logging.info('PARSE COMPLETED: {n} cases, {t} seconds'.format(n=len(new_listings), t=time.time()-t0))
