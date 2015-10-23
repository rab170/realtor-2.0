import os
import time
import errno
import string
import urllib2
import urlparse
import feedparser
from re import sub
from decimal import Decimal
from bs4 import BeautifulSoup

def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc: 
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise

class AptFeed(): 

    feed = None
    nan  = float('nan')
    no_geo = (nan, nan)

    #TODO finish process feed function to automate feeding and checking whether posts have been seen before
    #TODO add postgresql server to store all data (and to acomplish todo above ^^)
    #TODO hide http requests behind array of proxies (or tor)

    # add lots of fields...lots
    # save raw html for NLP, date posted, date modified, number of images, pixle density of images, size of images, distance from subway, yelp reviews, types of stores near by...

    def __init__(self, rss_url, img_base, soup_parser='lxml'):
        self.rss_url = rss_url
        self.img_base = img_base
        self.soup_parser = soup_parser
        #self.update_feed()
    
    def update_feed(self):
        self.feed = feedparser.parse(self.rss_url, modified=(self.feed.modified if self.feed != None else None))
        return self.feed.status

    def process_feed(self):
        for item in feed['items']:
            soup = self.get_soup(item['link'])

    def get_rent(self, soup):
        price = soup.find('span', {'class':'price'}).text
        return self.str_to_dec(price)

    def get_geo(self, soup):

        map_div = soup.find('div', {'id':'map'})
        geo_keys = ['data-latitude', 'data-longitude']

        if map_div == None or any(key not in map_div.attrs for key in geo_keys): return self.no_geo

        cords = map(map_div.attrs.get, geo_keys)
        geo = tuple( self.str_to_dec(cord) for cord in cords )
        return geo

    def get_size_metrics(self, soup):

        available_metrics = {}
        size_metrics = ['ft2', 'br']

        housing_info = soup.find('span', {'class':'housing'}).text
        if housing_info == None: return {}

        housing_info = [part.strip() for part in  housing_info.split('-')]
        for info in housing_info:
            for metric in size_metrics:
                if metric in info:
                    available_metrics[metric] = self.force_int( info.replace(metric, '') )
        return available_metrics
        
    def save_images(self, soup):
        thumbs = soup.find('div', {'id':'thumbs'}).findAll('a')
        if thumbs == None: return 0

        base_dir = os.path.join(self.img_base, self.get_post_id(soup)) 
        mkdir_p(base_dir)
        for i, thumb in enumerate(thumbs):
            img_url = thumb['href'] 
            img_data = urllib2.urlopen(img_url).read() 
            img_path = urlparse.urlparse(img_url).path 
            img_ext =  os.path.splitext(img_path)[1]

            path = os.path.join(base_dir, '{0}{1}'.format(i, img_ext))
            f = open(path, 'wb')
            f.write(img_data)
        return i

    def get_post_id(self, soup):
        text = soup.find('div', {'class':'postinginfos'}).findAll('p', {'class':'postinginfo'})[0].text
        return str(text).translate(None, string.whitespace).split(':')[-1]

    def get_soup(self, url):
        html = urllib2.urlopen(url).read()
        return BeautifulSoup(html, self.soup_parser)

    def str_to_dec(self, string):
        return Decimal(sub(r'[^\d.]', '', string))

    def force_int(self, string):
        return int(sub(r'[^\d]', '', string))   #CL seems to enforce ints for price and square footage. Taking the risk for now. Just want it running

if __name__ == '__main__':
    
    link = r'https://newyork.craigslist.org/search/aap?format=rss&hasPic=1'     # this forces the listing to have a picture AND square footage
                                                                                # consider changing to allow for no square footage...much harder
                                                                                # to extract bedroom and size metrics..but may lose some gems
    #TODO move this to a unittest suite 
    #TODO you should be ashamed of yourself 
    #TODO YOU HEATHEN

    parser = AptFeed(link, os.path.expanduser('~/img_tmp') )
    a = 'https://newyork.craigslist.org/mnh/fee/5280880697.html'
    b = 'https://newyork.craigslist.org/mnh/fee/5280880497.html'
    c = 'https://newyork.craigslist.org/que/fee/5280918276.html'
    urls = [a, b, c]
    for url in urls:
        soup = parser.get_soup(url)
        print parser.get_rent(soup)
        print parser.get_geo(soup)
        print parser.get_post_id(soup)
        print parser.get_size_metrics(soup)
        print parser.save_images(soup)

