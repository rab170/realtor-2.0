
#!/usr/bin/env python

import sys
import yaml
import logging
import pymongo
import pandas as pd

from scraper.Parser import Gesucht
from scraper.mongo_handler import Mongo

if __name__ == '__main__':

    with open(sys.argv[1]) as f:
        config = yaml.load(f)

    parser = Gesucht(config)
    mongo = Mongo(config['mongo'])

    fields = {'_id': 1, 'name': 1, 'lat': 1, 'lng': 1, 'user': 1}
    destinations = list(mongo.db['destinations'].find({}, fields))
    destinations = pd.DataFrame(destinations)

    try:
        with mongo.collection.watch([{'$match': {'operationType': 'insert'}}]) as stream:
            for insertion in stream:
                listing = insertion['fullDocument']
                origin = {k: listing[k] for k in ['_id', 'url', 'lat', 'lng']}

                successes = 0
                for idx, destination in destinations.iterrows():
                    try:
                        routes = parser.vbb.get_routes(origin, destination)
                        mongo.db['travel_routes'].insert_one({'origin': dict(origin),
                                                              'destination': dict(destination),
                                                              'user': destination.user,
                                                              'routes': routes})
                        successes+=1
                    except Exception as e:
                        logging.error('encountered an unknown while parsing VBB routes')
                        logging.error(e)

                logging.info('successuflly parsed {n} out of {total} routes for {url}'.format(n=successes,
                                                                                              total=len(destinations),
                                                                                              url=origin['url']))

    except pymongo.errors.PyMongoError as e:
        logging.error(e)
