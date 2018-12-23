import logging
from pymongo import MongoClient

class Mongo(object):

    def __init__(self, config):

        database = config.pop('database')
        collection = config.pop('collection')

        client = MongoClient(**config)
        db = client[database]

        if not collection in db.list_collection_names():
            db[collection].create_index('url', unique=True)

        self.db = db
        self.client = client
        self.collection = db[collection]

    def insert(self, document):
        if isinstance(document, dict):
            self.collection.insert_one(document)
        elif isinstance(document, list) and all([isinstance(item, dict) for item in document]):
            self.collection.insert_many(document)
        else:
            raise Exception('Document type {t} is not a dict or list of dicts'.format(t=type(document)))
        logging.info('{url} inserted to mongo'.format(url=document['url']))

    def get_existing_urls(self, urls):
        existing = self.collection.find({'url': {"$in": urls}})
        return [document['url'] for document in existing]
