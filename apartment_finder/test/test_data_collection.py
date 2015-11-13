import unittest
from ..data_collection.craigslist_rss import AptFeed

class TestRSS(unittest.TestCase):

    def setUp(self):
        link = r'https://newyork.craigslist.org/search/aap?format=rss&hasPic=1&minSqft=1'
        self.parser = AptFeed(link, '~/img', '~/proxies', 'apartment_listings','an0nym1ty', default_table='test')
        self.parser.update_feed()
        cursor = self.parser.pgSQL.pg_conn.cursor()
        try:
            cursor.execute('drop table test');
        except:
            self.parser.pgSQL.pg_conn.rollback()
        self.parser.pgSQL.init_db()

    def tearDown(self):
        self.parser.pgSQL.pg_conn.commit()
        del self.parser

    def test_coalesce_metrics(self):
        for item in self.parser.feed['items']:
            soup = self.parser.soup(item['link'])
            metrics = self.parser.coalesce_metrics(soup)
            for (k, v) in metrics.items():
                self.assertTrue(type(v) in self.parser.pgSQL.POSTGRESQL_TYPES)
             
    def test_primary_key(self):
        for item in self.parser.feed['items']:
            soup = self.parser.soup(item['link'])
            pk = self.parser.primary_key(soup)
            self.assertEqual( len(pk.items()), len(self.parser.primary_key_methods.keys()))
        
    def test_insert_real_data(self):
        identifiers = [] 
        for item in self.parser.feed['items']:
            soup = self.parser.soup(item['link'])
            metrics = self.parser.coalesce_metrics(soup)
            self.parser.pgSQL.insert(metrics)
            identifiers.append( {'url':metrics['url'], 'title':metrics['title'] })

        for db_identifier in identifiers:
            apt_exists = self.parser.pgSQL.apt_exists(db_identifier)
            self.assertTrue(apt_exists)

    def test_archive_real_data(self):
        identifiers = [] 
        for item in self.parser.feed['items']:
            soup = self.parser.soup(item['link'])
            metrics = self.parser.coalesce_metrics(soup)
            self.parser.pgSQL.insert(metrics)
            identifiers.append( {'url':metrics['url'], 'title':metrics['title'] })

        cursor =  self.parser.pgSQL.pg_conn.cursor()
        for db_identifier in identifiers:
            where_clause = self.parser.pgSQL.unique_where(db_identifier)
            query = u'select archived from {0} {1}'.format(self.parser.pgSQL.default_table, where_clause)
            cursor.execute(query)
            initial_archived = cursor.fetchone()[0]
            print initial_archived
            self.assertFalse(initial_archived)

        cursor =  self.parser.pgSQL.pg_conn.cursor()
        for db_identifier in identifiers:
            self.parser.pgSQL.archive_listing(db_identifier)
            where_clause = self.parser.pgSQL.unique_where(db_identifier)
            query = u'select archived from {0} {1}'.format(self.parser.pgSQL.default_table, where_clause)
            cursor.execute(query)
            new_archived = cursor.fetchone()[0]
            print new_archived
            self.assertTrue(new_archived)




