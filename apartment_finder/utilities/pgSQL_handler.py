import math
import inspect
import psycopg2
from types import NoneType
from datetime import datetime
from psycopg2 import errorcodes
from general_utils import ProxyHandler

def pgSQL_type_conversion(f):
    conversion_type = f.__name__.replace('convert_', '')
    exec( 't = {0}'.format(conversion_type) )
    f._conversionType = t
    return f

class pgSQL(object):

    pg_conn = None
    pgSQL_conversion_methods = {}

    null = 'NULL'
    nan = 'nan'
    no_geo = {'latitude':nan, 'longitude':nan}
    str_token = '$TOKEN$'    # quote character to wrap text field strings (eg: $TOKEN$ "quotes" can't cause any "problems" $TOKEN$)
    
    POSTGRESQL_TYPES = {int:'int4', float:'float4', str:'text', unicode:'text', datetime:'timestamp', bool:'bool'}

    def __init__(self, db_name, db_user, default_table='apartment_listings'):
        self.default_table = default_table
        self.pg_conn = psycopg2.connect('dbname={0} user={1}'.format(db_name, db_user))
        for (name, method) in inspect.getmembers(self, predicate=inspect.ismethod):
            if '_conversionType' in dir(method):
                self.pgSQL_conversion_methods[ method._conversionType ] = method


    def insert(self, metrics):
        """
            insert dictionary of metrics into the specified table 

            columns are added to PostgreSQL table if they are not present

            metrics: dictionary of key/value pairs for database. Keys MUST be STRINGS
            table:   string name of database table for metrics to be inserted

        """

        insert = u'insert into {0} {1} values {2}'
        vals = [ self.pgSQL_convert( metrics[key] ) for key in metrics.keys() ]
        keys_str = '(' + ','.join(metrics.keys()) + ')'
        vals_str = '(' + ','.join(vals) + ')'
        query = insert.format(self.default_table, keys_str, vals_str)
        cursor = self.pg_conn.cursor()
        try:
            cursor.execute(query)
        except Exception as e:
            self.pg_conn.rollback()
            if str(e.pgcode) == str(psycopg2.errorcodes.UNDEFINED_COLUMN):
                missing_field_names = self.identify_missing(metrics.keys())
                missing_fields = { key:type(metrics[key]) for key in missing_field_names }
                success = self.add_columns(missing_fields)
                if success:
                    cursor = self.pg_conn.cursor()
                    cursor.execute(query)
                else:
                    return False
            elif str(e.pgcode) == str(psycopg2.errorcodes.UNIQUE_VIOLATION):
                return False
            else:
                raise
        self.pg_conn.commit()
        return True

    def add_columns(self, fields):
        """
            adds columns to self.default_table based on dictionary of name/type pairs

            keys become column names, types become PostgreSQL types (based on the  type map found 
            in self.POSTGRESQL_TYPES)

            fields: dictionary mapping column names to python types

        """
        alter = 'alter table {0} add {1} {2}'
        for (name, field_type) in fields.items():
            pgSQL_type = self.POSTGRESQL_TYPES[field_type]
            query = alter.format(self.default_table, name, pgSQL_type)
            cursor = self.pg_conn.cursor()
            try:
                cursor.execute(query)
                self.pg_conn.commit()
            except Exception as e:
                self.pg_conn.rollback()
                raise
        return True

    def identify_missing(self, fields):
        """
                Identifies columns which do not exist for a given table

                Args:
                                fields  -- a list of column names

                Returns:
                                a list of dataabase field names which were not found in self.database

        """

        missing_fields = []
        select = 'select {0} from {1};'
        cursor = self.pg_conn.cursor()
        for field in fields:
            query = select.format(field, self.default_table)
            try:
                cursor.execute(query)
            except Exception as e:
                self.pg_conn.rollback()
                if str(e.pgcode) == str(psycopg2.errorcodes.UNDEFINED_COLUMN):
                    missing_fields.append(field)
                else:
                    raise
        return missing_fields

    def get_active_listings(self, column_selection):
        """
        """
        column_selection_string = ','.join(column_selection)
        query = 'select {0} from {1} where archived = False'.format(column_selection_string, self.default_table)
        cursor = self.pg_conn.cursor()
        cursor.execute(query)
        return cursor.fetchall()

    def archive_listing(self, identifier):
        """
            sets a listing's archived field to True

            identifier -- dictionary of key/value pairs that identify a given listing
        """
        cursor = self.pg_conn.cursor()
        where_clause = self.unique_where(identifier)
        query = u'update {0} set archived = True {1}'.format(self.default_table, where_clause)
        cursor.execute(query)
        self.pg_conn.commit()
        return True

    def apt_exists(self, identifier):
        cursor = self.pg_conn.cursor()
        where_clause = self.unique_where(identifier)
        query = u'select exists(select 1 from {0} {1})'.format(self.default_table, where_clause)
        try:
            cursor.execute(query)
        except Exception as e:
            self.pg_conn.rollback()
            if str(e.pgcode) == str(psycopg2.errorcodes.UNDEFINED_COLUMN):
                return False
            raise
        return cursor.fetchone()[0]


    def get_uid(self, identifier):
        where_clause = self.unique_where(identifier)
        query = u'select listing_number from {0} {1}'.format(self.default_table, where_clause)
        cursor = self.pg_conn.cursor()
        cursor.execute(query)
        return cursor.fetchone()[0]

    def unique_where(self, identifier):
        for (k, v) in identifier.items():
            if type(v) == str:
                v = unicode(v, 'utf-8')
            identifier[k] = self.pgSQL_convert(v)
        return u'where ' + u' and '.join([u'{0} = {1}'.format(k, v) for (k, v) in identifier.items()])

    @pgSQL_type_conversion
    def convert_str(self, val):
        if type(val) != str: return self.null
        return self.str_token + val + self.str_token

    @pgSQL_type_conversion
    def convert_bool(self, val):
        if type(val) != bool: return self.null
        # str(True) = 'True'
        # str(False) = 'False'
        return str(val)

    @pgSQL_type_conversion
    def convert_unicode(self, val):
        if type(val) != unicode: return self.null
        return self.str_token + val + self.str_token

    @pgSQL_type_conversion
    def convert_int(self, val):
        if type(val) != int: return self.null
        return str(val)

    @pgSQL_type_conversion
    def convert_float(self, val):
        if type(val) != float: return self.null
        if math.isnan(val): return self.nan
        return str(val)

    @pgSQL_type_conversion
    def convert_NoneType(self, val):
        return self.null

    @pgSQL_type_conversion
    def convert_datetime(self, val):
        if type(val) != datetime: return self.null
        f = '%Y-%m-%d %H:%M:%S'
        return self.str_token + val.strftime(f) + self.str_token

    def pgSQL_convert(self, val):
        conversion = self.pgSQL_conversion_methods[ type(val) ]
        return conversion(val)
    
    def init_db(self):
        cursor = self.pg_conn.cursor()
        cursor.execute('create table {0}( cl_id int8, listing_number serial8 )'.format(self.default_table))
        self.pg_conn.commit()

