import re
import time
import inspect
import urllib
import logging
import urllib.parse
import googlemaps
from functools import reduce
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from abc import ABCMeta, abstractmethod
from utils import ProxyHandler


def metric(f):
    def g(self, soup):
        try:
            return f(self, soup)
        except Exception as e:
            logging.error(e)
        return {}

    g.is_metric = True
    return g


class Parser(object):
    __metaclass__ = ABCMeta

    timeout = 5
    proxy_handler = None
    bs4_parsing = 'html5lib'

    def __init__(self, config):
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')

        self.selenium = webdriver.Chrome(executable_path=config['webdriver_path'], chrome_options=options)

        methods = inspect.getmembers(self, predicate=inspect.ismethod)
        self.metric_methods = [method for (name, method) in methods if 'is_metric' in dir(method)]
        self.gmaps = googlemaps.Client(key=config['google_api_key'])
        self.search_urls = config['searches']

    def parse_listing(self, url):
        soup = self.soup(url)
        metrics = map(lambda f: f(soup), self.metric_methods)
        return reduce(lambda a, b: dict(a, **b), metrics)

    def soup(self, url):
        try:
            if self.proxy_handler:
                proxy = self.proxy_handler.get_proxy()
                html = proxy.open(url).read()
            else:
                html = urllib.request.urlopen(url).read()
        except urllib.error.HTTPError as e:
            logging.error('failed to parse BeautifulSoup for {url}. \n'
                          'Encountered error: {e}'.format(url=url, e=e))
            return None

        soup = BeautifulSoup(html, self.bs4_parsing)
        soup.attrs['url'] = url
        return soup

    @abstractmethod
    def get_listings(self, listing_url):
        return []

    def __exit__(self, exc_type, exc_value, traceback):
        self.selenium.close()


class Gesucht(Parser):

    listings_per_page = 20
    domain = 'https://www.wg-gesucht.de'

    def get_listings(self, n=listings_per_page):
        listings = set()

        for url in self.search_urls:
            n_pages = int(n/self.listings_per_page) + 1
            for page in range(n_pages):
                page = self.soup(url.format(page=page))
                ad_ids = page.findAll('tr', {'adid': re.compile(r'.*')})
                ad_ids = map(lambda listing: listing['adid'], ad_ids)
                ad_urls = map(lambda relative_url: urllib.parse.urljoin(self.domain, relative_url), ad_ids)

                # Drop first 2 listings. They're ads
                listings = listings.union(list(ad_urls)[2:])
        return list(listings)[:n]

    @metric
    def get_url(self, soup):
        return {'url': soup.attrs['url']}

    @metric
    def get_rent(self, soup):
        names = ['rent', 'utilities', 'other', 'deposit']
        values = soup.find('div', {'class': 'col-sm-5'}).findAll('b')[0:len(names)]
        values = map(lambda s: s.text.replace(u'\u20ac', ''), values)
        values = map(lambda s: s.replace('n.a.', '0'), values)

        return {k: int(v) for k, v in zip(names, values)}

    @metric
    def get_size_metrics(self, soup):
        def f(s): return re.sub('\s\s+', '', s.text)

        k = u'Zimmergr\xf6\xdfe'

        key_facts = soup.findAll('h2', {'class': 'headline headline-key-facts'})
        fact_types = soup.findAll('h3', {'class': 'headline headline-key-facts'})

        res = {f(k): f(v) for k, v in zip(fact_types, key_facts)}
        res[k] = int(res[k].replace(u'm\xb2', ''))

        return {'square_meters': res[k]}

    @metric
    def get_wg_info(self, soup):
        def clean(s): return re.sub('\s\s+', '', s.text)
        divs = soup.findAll('div', {'class': 'col-sm-6'})
        names = map(lambda element: element.find('h4', {'class': 'headline headline-detailed-view-datasheet'}), divs)
        names = map(clean, names)
        names = list(names)

        div = divs[names.index(u'Die WG')]
        wg_details = div.find('ul', {'class': 'ul-detailed-view-datasheet print_text_left'}).findAll('li')
        wg_details = map(clean, wg_details)

        regex = {'WG_size': re.compile(r'(?P<WG_size>[0-9])er WG'),
                 'languages': re.compile(r'Sprache/n:(?P<languages>.*)')}
        processing = {'WG_size': int, 'languages': lambda s: s.split(',')}

        res = {}
        for backref, pattern in regex.items():
            hits = map(lambda s: pattern.search(s), wg_details)
            hits = filter(lambda hit: hit is not None, hits)
            hits = map(lambda hit: hit.group(backref), hits)

            hits = set(hits)
            if len(hits) == 1:
                res[backref] = processing[backref](hits.pop())
        return res

    @metric
    def get_location(self, soup):
        div = soup.find('div', {'class': 'col-sm-4 mb10'})
        address = div.find('a').text
        address = re.sub('\s\s+', ' ', address).strip()

        geocode_result = self.gmaps.geocode(address)
        geo = geocode_result[0]['geometry']['location']

        # (geo['lat'], geo['lng'])
        return {'address': address, 'lat': geo['lat'], 'long': geo['lng']}

    @metric
    def get_listing_info(self, soup):
        divs = soup.findAll('div', {'class': 'col-xs-12'})
        h3s = map(lambda div: div.find('h3', {'class': 'headline headline-detailed-view-panel-title'}), divs)
        h3s = map(lambda h3: h3.text.strip() if h3 else h3, h3s)
        h3s = list(h3s)

        description = divs[h3s.index(u'Anzeigentext')]
        description = re.sub('\s\s+', '\n', description.text.strip())

        details = divs[h3s.index(u'Angaben zum Objekt')]
        details = details.findAll('div', {'class': 'col-xs-6 col-sm-4 text-center print_text_left'})
        details = map(lambda item: re.sub('\s\s+', ' ', item.text).strip(), details)

        return {'description': description, 'details': details}

    @metric
    def get_img_urls(self, soup):
        url = soup.attrs['url']
        self.selenium.get(url)

        try:
            pointer = WebDriverWait(self.selenium, self.timeout).until(
                      EC.visibility_of_element_located((By.CLASS_NAME, 'cursor-pointer')))
            pointer.click()
        except Exception as e:
            logging.info('no "weiter zu den Bilder" tab')
        finally:
            urls = []
            desc = []

            time.sleep(self.timeout)

            thumbnails = self.selenium.find_elements_by_class_name('sp-thumbnail')
            if len(thumbnails):
                urls = map(lambda thumbnail: thumbnail.get_attribute('src'), thumbnails)
                urls = map(lambda s: s.replace('thumb', 'large'), urls)
                desc = map(lambda thumbnail: thumbnail.get_attribute('alt'), thumbnails)
            else:
                thumbnails = self.selenium.find_elements_by_class_name('sp-image')[1:]
                if len(thumbnails):
                    urls = map(lambda thumbnail: thumbnail.get_attribute('src'), thumbnails)
                    desc = map(lambda thumbnail: thumbnail.get_attribute('alt'), thumbnails)

        pairs = zip(urls, desc)
        pairs = filter(lambda item: item[0] != u'https://www.wg-gesucht.de/img/d.gif', pairs)
        urls, desc = map(list, zip(*pairs))

        return {'image_urls': urls, 'image_descriptions': desc}
