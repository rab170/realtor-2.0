import math
import json
import inspect
import psycopg2
from datetime import datetime
from psycopg2 import errorcodes


def conversion(f):
    type_string = f.__name__.replace('convert_', '')
    conversion_type = eval(type_string)

    def g(self, val):
        if type(val) != conversion_type:
            return None
        return f(self, val)

    g._conversionType = conversion_type
    return g



class pgSQL(object):
    pg_conn = None
    conversion_methods = {}

    nan = 'nan'
    str_token = '$TOKEN$'

    TYPES = {int: 'int4', list: 'text', float: 'float4', str: 'text', 
             datetime: 'timestamp', bool: 'bool'}

    def __init__(self, db_config, table='hamburg'):
        self.table = table
        self.pg_conn = psycopg2.connect(**db_config)
        for (name, method) in inspect.getmembers(self, predicate=inspect.ismethod):
            if '_conversionType' in dir(method):
                self.conversion_methods[method._conversionType] = method

    def insert(self, metrics):
        """
            insert dictionary of metrics into the specified table

            columns are added to PostgreSQL table if they are not present

            metrics: dictionary of key/value pairs for database. Keys MUST be STRINGS
            table:   string name of database table for metrics to be inserted

        """

        insert = u'insert into {table} ({fields}) values ({values})'
        fields = metrics.keys()
        values = map(self.convert, metrics.values())

        query = insert.format(table=self.table, fields=','.join(fields), values=','.join(values))
        cursor = self.pg_conn.cursor()

        try:
            cursor.execute(query)
        except Exception as e:
            self.pg_conn.rollback()
            if str(e.pgcode) == str(psycopg2.errorcodes.UNDEFINED_COLUMN):
                missing_field_names = self.identify_missing(metrics.keys())
                missing_fields = {key: type(metrics[key]) for key in missing_field_names}
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
            adds columns to self.table based on dictionary of name/type pairs

            keys become column names, types become PostgreSQL types (based on the  type map found
            in self.POSTGRESQL_TYPES)

            fields: dictionary mapping column names to python types

        """
        alter = 'alter table {0} add {1} {2}'
        for (name, field_type) in fields.items():
            query = alter.format(self.table, name, self.TYPES[field_type])
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

                Args:     a list of column names

                Returns:  a list of dataabase field names which were not found in self.database

        """

        missing_fields = []
        select = 'select {0} from {1};'
        cursor = self.pg_conn.cursor()
        for field in fields:
            query = select.format(field, self.table)
            try:
                cursor.execute(query)
            except Exception as e:
                self.pg_conn.rollback()
                if str(e.pgcode) == str(psycopg2.errorcodes.UNDEFINED_COLUMN):
                    missing_fields.append(field)
                else:
                    raise
        return missing_fields

    def get_active_listings(self):
        query = 'select url from {table} where archived = False order by timestamp'.format(table=self.table)
        cursor = self.pg_conn.cursor()
        cursor.execute(query)
        return list(x[0] for x in cursor.fetchall())

    def set_archived(self, url):
        cursor = self.pg_conn.cursor()
        update = u"update {table} set archived = TRUE where url = '{url}'".format(table=self.table, url=url)
        try:
            cursor.execute(update)
            self.pg_conn.commit()
        except Exception as e:
            self.pg_conn.rollback()
            raise
        return True

    @conversion
    def convert_list(self, val):
        return self.str_token + json.dumps(val) + self.str_token

    @conversion
    def convert_map(self, val):
        return self.str_token + json.dumps(list(val)) + self.str_token

    @conversion
    def convert_str(self, val):
        return self.str_token + val + self.str_token

    @conversion
    def convert_bool(self, val):
        return str(val)

    @conversion
    def convert_int(self, val):
        return str(val)

    @conversion
    def convert_float(self, val):
        if math.isnan(val):
            return self.nan
        return str(val)

    @conversion
    def convert_datetime(self, val):
        format_str = '%Y-%m-%d %H:%M:%S'
        return self.str_token + val.strftime(format_str) + self.str_token

    def convert(self, val):
        f = self.conversion_methods[type(val)]
        return f(val)



