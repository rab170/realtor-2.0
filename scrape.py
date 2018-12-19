#!/usr/bin/env python

import sys
import time
import yaml
import random
import logging

from Parser import Gesucht
from pgSQL_handler import pgSQL

if __name__ == '__main__':

    with open(sys.argv[1]) as f:
        config = yaml.load(f)

    parser = Gesucht(config)
    SQL = pgSQL(config['postgreSQL'])

    listings = parser.get_listings(n=60)
    existing_listings = SQL.get_active_listings()
    new_listings = set(listings) - set(existing_listings)
    t0 =  time.time()

    for listing in new_listings:
        metrics = parser.parse_listing(listing)
        logging.info('{url} parsed'.format(url=listing))
        SQL.insert(metrics)
        logging.info('{url} inserted to postgreSQL'.format(url=listing))
        time.sleep(random.uniform(0, 3))
    logging.info('PARSE COMPLETED: {n} cases, {t} seconds'.format(n=len(new_listings), t=time.time()-t0))
