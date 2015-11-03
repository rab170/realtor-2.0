import math
import shutil
import socket
import random
import psycopg2
import dateutil.parser
from types import NoneType
from bs4 import BeautifulSoup
from datetime import datetime
from psycopg2 import errorcodes
import os, time, errno, string, inspect, urllib2, urlparse, feedparser, re



class ProxyHandler:

    def __init__(self, proxy_list_path):
        self.N = -1
        if os.path.exists(proxy_list_path):
            with open(proxy_list_path) as f:
                self.proxies = [ {'ip':l.split()[0], 'port':l.split()[1]} for l in f.readlines() ]
            self.N = len(self.proxies)
        
    def get_proxy(self):
        if self.N >= 0:
            index = random.randint(0, self.N - 1)
            proxy = self.proxies[index]
            return self.get_opener(proxy['ip'], proxy['port'])
        return False

    def get_opener(self, ip, port, auth=None):
        proxy_string = 'http://{0}:{1}'.format(ip, port)
        proxy = urllib2.ProxyHandler({'http':proxy_string})
        opener = urllib2.build_opener(proxy)

        """
        request = opener.open('http://checkip.dyndns.org').read()
        ext_ip = re.findall(r"\d{1,3}\.\d{1,3}\.\d{1,3}.\d{1,3}", request)[0]
        if ext_ip != ip:
            return False 
        """

        return opener

def metric(f):
    f.is_metric = True
    return f

def pgSQL_type_conversion(f):
    conversion_type = f.__name__.replace('convert_', '')
    exec( 't = {0}'.format(conversion_type) )
    f.__conversionType__ = t
    return f

def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc: 
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise

#TODO good comments (see: https://google-styleguide.googlecode.com/svn/trunk/pyguide.html#Comments)  + UnitTest suite
#TODO add fields

class AptFeed(object): 

    """
    Loop over a given CraigsList RSS feed for Apartment Listings and save metrics that predict property value to a PostgreSQL database

    @metric functions:
        these accept a BeautifulSoup (html) object and return a dictionary whose keys map to the postgresql database fields, and values are that listing's coresponding values

        this convention allows new metrics to be added to the AptFeed class without any additional overhead. All metrics can be automatically called, and new db fields
        can be automatically added

    """

    null    = 'NULL'
    nan     = float('nan')
    no_geo  = {'latitude':nan, 'longitude':nan}
    str_token = '$TOKEN$'   # used as quote character to wrap Postgre text field strings (eg: $TOKEN$ "quotes" can't cause any "problems" $TOKEN$)

    feed = None
    pg_conn = None
    postgresql_types = {int:'int4', float:'float4', str:'text', unicode:'text', datetime:'timestamp'}
    ACTIVE_LISTINGS = 'active_listings'

    wait_time = 5

    def __init__(self, rss_url, base_dir, proxy_path='', db_name=None, db_user=None, soup_parser='lxml'):
        self.rss_url = rss_url
        self.soup_parser = soup_parser
        self.filesystem_base = os.path.expanduser(base_dir)
        self.proxy_handler = ProxyHandler(os.path.expanduser(proxy_path))

        methods = inspect.getmembers(self, predicate=inspect.ismethod)
        self.metric_methods = [method for (name, method) in methods if 'is_metric' in dir(method)]
        self.pgSQL_conversion_methods = { method.__conversionType__:method for (name, method) in methods if '__conversionType__' in dir(method)}

        if db_name != None and db_user != None:
            self.pg_conn = psycopg2.connect('dbname={0} user={1}'.format(db_name, db_user))

    def process_feed(self):
        while True:
            self.update_feed()
            t_start = time.time()
            for item in self.feed['items']:
                if not self.pgSQL_apt_exists(item['link']):
                    soup = self.soup(item['link'])
                    metrics = self.coalesce_metrics(soup)
                    self.pgSQL_insert(metrics, self.ACTIVE_LISTINGS)
                    time.sleep(random.normalvariate(self.wait_time, self.wait_time*0.25))
            elapsed = time.time() - t_start;
            if elapsed < 7*60:
                time.sleep(7*60 - elapsed)

    def update_feed(self):
        self.feed = feedparser.parse(self.rss_url, modified=(self.feed.modified if self.feed != None else None))
        return self.feed.status

    def soup(self, url):
        proxy = self.proxy_handler.get_proxy()
        if proxy:
            html = proxy.open(url).read()
            return BeautifulSoup(html, self.soup_parser)
        return None

    @metric
    def get_rent(self, soup):
        rent = soup.find('span', {'class':'price'}).text
        rent = re.sub('[^0-9]', '', rent)
        rent = int(rent)
        return {'rent':rent}

    @metric
    def get_geo(self, soup):
        map_div = soup.find('div', {'id':'map'})
        geo_keys = ['data-latitude', 'data-longitude']  # this div also contains an "accuracy" metric -- maybe useful, probably not
        if map_div == None or any(key not in map_div.attrs for key in geo_keys): return self.no_geo 
        return { k.replace('data-', ''):self.str_to_float(map_div[k]) for k in geo_keys}

    @metric
    def get_size_metrics(self, soup):
        db_fields = {}
        size_metrics = {'ft2':None, 'br':None}
        housing_info = soup.find('span', {'class':'housing'}).text
        if housing_info == None: return {}
        for text in housing_info.split('-'):
            for metric in size_metrics:
                if metric in text:
                    text = text.replace(metric, '')     
                    text = re.sub('[^0-9]', '', text)
                    db_fields[metric] = int(text)
        return db_fields
        
    @metric
    def get_scrape_time(self, soup):
        return {'scrape_time': datetime.now()}
    @metric 
    def get_url(self, soup):
        url = soup.link.attrs['href']
        return {'url':url}

    @metric
    def get_text_body(self, soup):
        body = soup.find('section', {'id':'postingbody'}).text
        return {'text_body':body}

    @metric
    def get_title(self, soup):
        title = soup.title.text
        return {'title':title}

    @metric
    def get_cl_id(self, soup):
        text = soup.find('div', {'class':'postinginfos'}).findAll('p', {'class':'postinginfo'})[0].text
        text = re.sub('[^0-9]', '', text)
        cl_id = int(text)
        return {'cl_id':cl_id}

    @metric
    def get_post_date(self, soup):
        date_str = soup.find('div', {'class':'postinginfos'}).findAll('p', {'class':'postinginfo'})[1].find('time').attrs['datetime']
        dt = dateutil.parser.parse(date_str)
        return {'created':dt}

    @metric
    def save_images(self, soup):
        thumbs = soup.find('div', {'id':'thumbs'})
        thumbs = thumbs.findAll('a') if thumbs != None else None
        if thumbs == None: return {'n_img':0}

        img_dir = str(self.get_cl_id(soup)['cl_id'])
        img_dir = os.path.join(self.filesystem_base, img_dir) 
        mkdir_p(img_dir)

        count = 0
        proxy = self.proxy_handler.get_proxy()
        for i, thumb in enumerate(thumbs):
            if proxy:
                img_data = proxy.open(thumb['href'] ).read()       #TODO hide with random list of proxies
                img_path = urlparse.urlparse(thumb['href']).path 
                img_ext =  os.path.splitext(img_path)[1]
                path = os.path.join(img_dir, '{0}{1}'.format(i, img_ext))
                with open(path, 'wb') as f:
                    f.write(img_data)
                    count+=1
        return {'n_img':count}

    def coalesce_metrics(self, soup):
        db_fields = {}
        for field in [self.null_if_error(f, soup) for f in self.metric_methods]:
            db_fields.update(field)
        return db_fields 

    def pgSQL_insert(self, metrics, table):

        insert = u'insert into {0} {1} values {2};'       # {0} = table_name,  {1} = '(key_0, key_1, ..., key_n)', {2} = '(val_0, val_1, ..., val_n)'
        vals = [ self.pgSQL_convert( metrics[key] ) for key in metrics.keys() ]
        keys_str = '(' + ','.join(metrics.keys()) + ')'
        vals_str = '(' + ','.join(vals) + ')'
        query = insert.format(table, keys_str, vals_str)

        try: 
            cursor = self.pg_conn.cursor()
            cursor.execute(query)
        except Exception as e:
            self.pg_conn.rollback()
            if str(e.pgcode) == str(psycopg2.errorcodes.UNDEFINED_COLUMN):
                missing_field_names = self.pgSQL_identify_missing(metrics.keys(), table)
                missing_fields = { key:type(metrics[key]) for key in missing_field_names } 
                success = self.pgSQL_add_columns( table, missing_fields)
                if success:
                    cursor = self.pg_conn.cursor()
                    cursor.execute(query)
            elif str(e.pgcode) == str(psycopg2.errorcodes.UNIQUE_VIOLATION):
                # TODO add support for update operations
                print psycopg2.errorcodes.lookup(e.pgcode)
                print metrics['url']
            else:
                raise

        self.pg_conn.commit()
        return True
       
    def pgSQL_add_columns(self, table, fields):
        alter = 'ALTER TABLE {0} ADD {1} {2}'
        for (name, field_type) in fields.items():
            pgSQL_type = self.postgresql_types[field_type]
            query = alter.format(table, name, pgSQL_type)
            try:
                cursor = self.pg_conn.cursor()
                cursor.execute(query)
            except Exception as e:
                self.pg_conn.rollback()
                raise
        self.pg_conn.commit()
        return True 

    def pgSQL_identify_missing(self, fields, table):
        """
            Identifies columns which do not exist for a given table
            
            Args: 
                    fields  -- a list of column names
                    table   -- name of the table to check 

            Returns:
                    a list of dataabase field names which were not found in self.database

        """

        missing_fields = []
        select = 'select {0} from {1};'
        for field in fields:
            query = select.format(field, table)
            try: 
                cursor = self.pg_conn.cursor()
                cursor.execute(query)
            except Exception as e:
                self.pg_conn.rollback()
                if str(e.pgcode) == str(psycopg2.errorcodes.UNDEFINED_COLUMN):
                    missing_fields.append(field)
                else:
                    raise
        return missing_fields

    def pgSQL_apt_exists(self, url):
        cl_id = url.split('/')[-1].replace('.html', '')
        if cl_id != '':
            query = 'select exists(select 1 from active_listings where cl_id = {0})'.format(cl_id)
            cursor = self.pg_conn.cursor()
            cursor.execute(query)
            return cursor.fetchone()[0]
        return False

    def post_removed(self, soup):
        removed = soup.find('div', {'class':'removed'})
        if removed == None:
            return False
        return True

    def reset_db(self):
        if os.path.exists(self.filesystem_base):
            shutil.rmtree(self.filesystem_base)
        cursor = self.pg_conn.cursor()
        cursor.execute('drop table {0}'.format(self.ACTIVE_LISTINGS));
        cursor.execute('create table {0}( cl_id int8 primary key, listing_number serial8 )'.format(self.ACTIVE_LISTINGS))
        self.pg_conn.commit()

    @pgSQL_type_conversion
    def convert_str(self, val):
        if type(val) != str: return self.null
        return self.str_token + val + self.str_token 
    
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
        if math.isnan(val): return self.str_token + 'NaN' + self.str_token 
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

    def str_to_float(self, string):
        return float(re.sub(r'[^\d.]', '', string))

    def null_if_error(self, f, soup):
        try:
            return f(soup)
        except:
            return self.null

if __name__ == '__main__':
    
    link = r'https://newyork.craigslist.org/search/aap?format=rss&hasPic=1&minSqft=1'       # this forces the listing to have a picture AND square footage
                                                                                            # consider changing to allow for no square footage...much harder
                                                                                            # to extract bedroom and size metrics..but may lose some gems
    #TODO move this to a unittest suite 
    #TODO you should be ashamed of yourself 
    #TODO YOU HEATHEN

    parser = AptFeed(link, '~/img', '~/proxies', 'apartment_listings','an0nym1ty' )
    parser.reset_db()
    parser.process_feed()
