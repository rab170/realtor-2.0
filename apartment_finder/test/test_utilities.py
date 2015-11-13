import inspect
import unittest
import datetime
from ..utilities.pgSQL_handler import pgSQL

class TestpgSQL(unittest.TestCase):

    def setUp(self):
        self.pgSQL = pgSQL('apartment_listings', 'an0nym1ty', default_table='test')
        cursor = self.pgSQL.pg_conn.cursor()
        try:
            cursor.execute('drop table test');
        except:
            self.pg_conn.rollback()
        self.pgSQL.init_db()

    def tearDown(self):
        self.pgSQL.pg_conn.commit()
        del self.pgSQL

    def test_init(self):
        self.assertTrue(self.pgSQL != None)
        self.assertTrue(self.pgSQL.pg_conn != None)
        
        n = 0
        for (name, method) in inspect.getmembers(self.pgSQL, predicate=inspect.ismethod):
            if '_conversionType' in dir(method):
                n+=1
        
        m = len(self.pgSQL.pgSQL_conversion_methods)
        self.assertEqual(m, n, msg='number of conversion methods inconsistent ({0} and {1})'.format(n, m))


    def test_identify_missing(self):
        metrics= {'a':25, 'b':3.5213, 'c':'this is a string', 'd':datetime.datetime.now(), 'e':True}

        drop = 'alter table {0} drop column {1}'
        table = self.pgSQL.default_table

        cursor = self.pgSQL.pg_conn.cursor()
        for column in metrics.keys():
            try:
                cursor.execute(drop.format(table, column))
                self.pgSQL.pg_conn.commit() 
            except:
                self.pgSQL.pg_conn.rollback() 

        missing_metrics = self.pgSQL.identify_missing(metrics.keys())
        self.assertEqual(metrics.keys(), missing_metrics)

    def test_insert(self):
        metrics= {'a':25, 'b':3.5213, 'c':'THIS"is|" some$TEXT$"BRO', 'd':datetime.datetime.now(), 'e':True}
        result = self.pgSQL.insert(metrics)
        missing_metrics = self.pgSQL.identify_missing(metrics.keys())
        self.assertEqual(missing_metrics, [])
        self.assertTrue(result, 'insert unique row returns false')

    def test_archive(self):
        identifier = {'b':32}
        metrics = {'a':True, 'b':32, 'archived':False}
        cursor =  self.pgSQL.pg_conn.cursor()
        where_clause = self.pgSQL.unique_where(identifier)

        self.pgSQL.insert(metrics)
        query = 'select archived from {0} {1}'.format(self.pgSQL.default_table, where_clause)
        cursor.execute(query)
        initial_archived = cursor.fetchone()[0]
        self.assertFalse(initial_archived)

        self.pgSQL.archive_listing(identifier)
        query = 'select archived from {0} {1}'.format(self.pgSQL.default_table, where_clause)
        cursor.execute(query)
        new_archived = cursor.fetchone()[0]
        self.assertTrue(new_archived)








