import os
import sys
import yaml
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from scraper.mongo_handler import Mongo

class TestMongo(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(TestMongo, self).__init__(*args, **kwargs)
        with open('../scraping/configs/hamburg.yaml', 'r') as f:
            config = yaml.load(f)

        self.mongo = Mongo(config['mongo'])

    def test_insert_one(self):
        listing = {'url': 'https://www.wg-gesucht.de/wg-zimmer-in-Hamburg-Winterhude.6856681.html',
                    'images': ['image1', 'image2', 'image3'],
                    'rent': 350,
                    'square_meters': 32}

        self.mongo.insert(listing)
        inserted = self.mongo.collection.find_one(listing)
        self.assertIsNotNone(inserted)

    def test_get_existing_urls(self):
        urls = ['a', 'b', 'c', 'd', 'e', 'f', 'g']
        documents = [{'url':v} for v in urls[:5]]
        self.mongo.insert(documents)
        existing_urls = self.mongo.get_existing_urls(urls)
        self.assertListEqual(existing_urls, urls[:5])

if __name__ == '__main__':
    unittest.main()
