import re
import os
import time
import string 
import random
import inspect
import urllib2
import urlparse
import feedparser
import dateutil.parser
from datetime import datetime
from bs4 import BeautifulSoup

from ..utilities.general_utils import mkdir_p, ProxyHandler
from ..utilities.pgSQL_handler import pgSQL

def metric(f):
    f.is_metric = True
    return f

class AptFeed(object):

    """
    Loop over a given CraigsList RSS feed for Apartment Listings and save metrics that 
    predict property value to a PostgreSQL database.

    @metric functions:
        These accept a BeautifulSoup (html) object and return a dictionary whose keys map to 
        the postgresql database fields, and values are that listing's coresponding values

        This convention allows new metrics to be added to the AptFeed class without any 
        additional overhead. All metrics can be automatically called, and new db fields
        can be automatically added.

    """


    feed = None
    pgSQL = None
    proxy_handler = None
    soup_parser = 'lxml'

    primary_key_methods = {} 

    DELTA_T = 5*60

    def __init__(self, rss_url, base_dir, proxy_path='', db_name=None, db_user=None, default_table='apartment_listings', soup_parser='lxml'):
        self.rss_url = rss_url
        self.soup_parser = soup_parser
        self.img_base = os.path.expanduser(base_dir)

        if proxy_path != '':
            self.proxy_handler = ProxyHandler(os.path.expanduser(proxy_path))
        if db_name != None and db_user != None:
            self.pgSQL = pgSQL(db_name, db_user, default_table=default_table)
        methods = inspect.getmembers(self, predicate=inspect.ismethod)
        self.metric_methods = [method for (name, method) in methods if 'is_metric' in dir(method)]
        self.primary_key_methods = {'url':self.get_url, 'title':self.get_title}

    def process_feed(self):
        while True:
            self.update_feed()
            t_start = time.time()
            for item in self.feed['items']:
                soup = self.soup(item['link'])
                db_identifier = self.primary_key(soup)
                if not self.pgSQL.apt_exists(db_identifier):
                    metrics = self.coalesce_metrics(soup)
                    self.pgSQL.insert(metrics)
                    listing_id = str(self.pgSQL.get_uid(db_identifier))
                    print 'inserted url={0}'.format(metrics['url'])
                    #self.save_images(soup, listing_id)
                else:
                    print 'url={0} ALREADY EXISTS '.format(db_identifier['url'])
            elapsed = time.time() - t_start;
            if elapsed < self.DELTA_T:
                time.sleep(self.DELTA_T - elapsed)

    def update_feed(self):
        self.feed = feedparser.parse(self.rss_url, modified=(self.feed.modified if self.feed != None else None))
        return self.feed.status

    def soup(self, url):
        try:
            if self.proxy_handler != None:
                proxy = self.proxy_handler.get_proxy()
                html = proxy.open(url).read()
            else:
                html = urllib2.urlopen(url).read()
        except urllib2.HTTPError as e:
            return None
        return BeautifulSoup(html, self.soup_parser)

    @metric
    def get_rent(self, soup):
        rent = soup.find('span', {'class':'price'}).text
        rent = re.sub('[^0-9]', '', rent)
        rent = int(rent)
        return {'rent':rent}

    @metric
    def get_geo(self, soup):
        map_div = soup.find('div', {'id':'map'})
        geo_keys = ['data-latitude', 'data-longitude']
        if map_div == None or any(key not in map_div.attrs for key in geo_keys): return self.pgSQL.no_geo
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
    def get_archived(self, soup):
        return {'archived':False}

    @metric
    def get_scrape_time(self, soup):
        return {'scrape_time': datetime.now()}

    @metric
    def get_post_date(self, soup):
        date_str = soup.find('div', {'class':'postinginfos'}).findAll('p', {'class':'postinginfo'})[1].find('time').attrs['datetime']
        dt = dateutil.parser.parse(date_str)
        return {'created':dt}

    def coalesce_metrics(self, soup):
        db_fields = {}
        for field in [self.catch_all(f, soup) for f in self.metric_methods]:
            db_fields.update(field)
        return db_fields

    def save_images(self, soup, img_dir):
        thumbs = soup.find('div', {'id':'thumbs'})
        thumbs = thumbs.findAll('a') if thumbs != None else None
        if thumbs == None: return False

        img_dir = os.path.join(self.img_base, img_dir)
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
        return True

    def listing_removed(self, soup):
        try:

            # This is a really sloppy way to determine if a post has been removed.
            # @metric methods shouldn't even be erroring under any condition. These things should
            # be handled. REASON: many different pages can crop up for "removed" posts. This
            # includes 404'd pages, removed by CL, and removed by author (maybe more)

            self.get_post_date(soup)

        except Exception as e:
            return True
        return False

    def archive(self):
        column_selection = ['url', 'title']
        for db_row in self.pgSQL.get_active_listings(column_selection):
            url = db_row[0] 
            title = db_row[1]
            soup = self.soup(url)
            removed = self.listing_removed(soup)
            if removed:
                self.pgSQL.archive_listing({'url':url, 'title':title})
                print 'archived url={0}'.format(url)

    def str_to_float(self, string):
        return float(re.sub(r'[^\d.]', '', string))

    def catch_all(self, f, soup):
        try:
            return f(soup)
        except Exception as e:
            print e
            return {}
        
    def primary_key(self, soup):
        """
            This methdo returns a dictionary of primary key and value pairs for the apartment
            listing specified by the input HTML soup

        """
        primary_keys = {}
        for field in [self.catch_all(f, soup) for f in self.primary_key_methods.values()]:
            primary_keys.update(field)
        return primary_keys

