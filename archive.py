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

    existing_listings = SQL.get_active_listings()

    for listing in existing_listings[0:100]:
        if parser.is_deactivated(listing):
            logging.info('archiving inactive listing {url}'.format(url=listing))
            SQL.set_archived(listing)
        time.sleep(random.uniform(0, 3))
