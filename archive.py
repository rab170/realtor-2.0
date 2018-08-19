#!/usr/bin/env python

import time
import yaml
import random
import logging

from Parser import Gesucht
from pgSQL_handler import pgSQL

if __name__ == '__main__':

    with open('config.yaml') as f:
        config = yaml.load(f)

    parser = Gesucht(config)
    SQL = pgSQL(config['postgreSQL'])

    existing_listings = SQL.get_active_listings()

    for listing in existing_listings[:100]:
        if parser.is_active(listing):
            logging.info('archiving inactive listing {url}'.format(url=listing))
            SQL.archive(listing)
        time.sleep(random.uniform(0, 3))
