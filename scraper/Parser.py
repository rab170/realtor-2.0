import re
import math
import time
import random
import urllib
import inspect
import logging
import requests
import googlemaps
import dateutil.parser

from itertools import cycle
from collections import OrderedDict

from functools import reduce
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from abc import ABCMeta, abstractmethod


def metric(f):
    def g(self, soup):
        try:
            return f(self, soup)
        except Exception as e:
            logging.error('Error in metric {name}: {error}'.format(name=f.__name__, error=e))
        return {}

    g.is_metric = True
    return g


class Captcha(Exception):
    def __init__(self):
        super().__init__('Captcha found in request.')


class Parser(object):
    __metaclass__ = ABCMeta

    timeout = 5
    proxies = None
    bs4_parsing = 'html.parser'

    SEARCH_RADIUS = 750
    MAX_DISTANCE = 3*SEARCH_RADIUS

    GOOGLE_PLACES_URL = 'www.google.com/maps/search/?api=1&query=Google&query_place_id={place_id}'
    GOOGLE_PLACES_FIELDS = ['name','types', 'place_id', 'distance', 'rating']


    def __init__(self, config):


        methods = inspect.getmembers(self, predicate=inspect.ismethod)
        self.metric_methods = [method for (name, method) in methods if 'is_metric' in dir(method)]

        if 'proxies' in config:
            proxies = config['proxies']
            proxies = [{'https': 'https://{proxy}'.format(proxy=proxy)} for proxy in proxies]
            self.proxies = cycle(proxies)

        if 'logfile' in config:
            logging.basicConfig(filename=config['logfile'],
                                format='%(asctime)s %(levelname)s %(message)s',
                                filemode='a+',
                                level=logging.INFO)

        if 'searches' in config:
            self.search_urls = config['searches']

        if 'google_api_key' in config:
            self.gmaps = googlemaps.Client(key=config['google_api_key'])

        if 'webdriver_path' in config:
            options = webdriver.ChromeOptions()
            options.add_argument('--headless')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            self.selenium = webdriver.Chrome(executable_path=config['webdriver_path'], chrome_options=options)

        if 'GOOGLE_SEARCHES' in config:
            self.GOOGLE_SEARCHES = config['GOOGLE_SEARCHES']

        if 'GOOGLE_SEARCH_TYPES' in config:
            self.GOOGLE_SEARCH_TYPES = config['GOOGLE_SEARCH_TYPES']

    def parse_listing(self, url):
        soup = self.soup(url)
        metrics = map(lambda f: f(soup), self.metric_methods)
        return reduce(lambda a, b: dict(a, **b), metrics)

    def soup(self, url):
        try:
            proxy = next(self.proxies) if self.proxies else {}
            request = requests.get(url, proxies=proxy)
            html = request.content
        except requests.exceptions.HTTPError as e:
            logging.error('failed to parse BeautifulSoup for {url}. \n'
                          'Encountered error: {e}'.format(url=url, e=e))
            if e.response.status_code == 403:
                logging.error('STATUS CODE 403 FORBIDDEN FOR {url} ON {proxy}'.format(url=url, proxy=proxy))
                raise
            return None

        soup = BeautifulSoup(html, self.bs4_parsing)
        soup.attrs['url'] = url
        if self.has_captcha(soup):
            logging.error('ENCOUNTERED CAPTCHA ON {proxy} fetching {url}'.format(url=url, proxy=proxy))
            raise Captcha()

        logging.info('{url} parsed'.format(url=url))
        time.sleep(self.timeout)
        return soup

    def google_places(self, apartment_coordinates):

        places = set(self.GOOGLE_SEARCHES).intersection(self.GOOGLE_SEARCH_TYPES)
        search_params = {name: (self.GOOGLE_SEARCHES[name], self.GOOGLE_SEARCH_TYPES[name]) for name in places}
        places = {k: [] for k in places}

        for name, (text, types) in search_params.items():
            for T in types:
                search = self.gmaps.places(text, type=T, location=apartment_coordinates, radius=self.SEARCH_RADIUS)

                for place in search['results']:

                    location = place['geometry']['location']
                    place['distance'] = self.coordinate_distance(location, apartment_coordinates)
                    place['url'] = self.GOOGLE_PLACES_URL.format(place_id=place['place_id'])

                    if place['distance'] <= self.MAX_DISTANCE:
                        fields = {field: place[field] for field in self.GOOGLE_PLACES_FIELDS if field in place}
                        places[name].append({**location, **fields})
                    places[name] = sorted(places[name], key=lambda place: place['distance'])

        return places

    @staticmethod
    def coordinate_distance(A, B):
        r_earth = 6371000
        deg = lambda rad: math.pi*rad/180

        delta = {}
        for dim in ['lat', 'lng']:
            delta[dim] = deg(A[dim] - B[dim])

        a = deg(A['lat'])*deg(B['lat'])
        b =   math.sin(delta['lat']/2)**2 + \
            a*math.sin(delta['lng']/2)**2
        c = 2*math.atan2(b**0.5, (1-b)**0.5)
        return r_earth*c

    @abstractmethod
    def get_listings(self, listing_url):
        return []

    @abstractmethod
    def has_captcha(self, soup):
        pass

    @abstractmethod
    def is_deactivated(self, url):
        pass

    def __del__(self):
        self.selenium.quit()

    def __exit__(self, exc_type, exc_value, traceback):
        self.selenium.quit()


class Gesucht(Parser):

    listings_per_page = 20
    domain = 'https://www.wg-gesucht.de'

    def is_deactivated(self, url):
        soup = self.soup(url)
        if soup is None:
            return True

        warning = soup.find('div', {'class': 'noprint alert alert-warning'})
        if warning is not None:
            children = filter(lambda item: 'text' in dir(item), warning.children)
            warnings = ' '.join(child.text for child in children)
            return 'deaktiviert' in warnings
        return False

    def get_listings(self, n=listings_per_page):

        listings = {}

        for url in self.search_urls:

            page_number = 0
            listings[url] = OrderedDict()

            while len(listings[url]) < n:
                page = self.soup(url.format(page=page_number))

                filters = {'end_date': {'class': 'ang_spalte_freibis row_click'},
                           'location': {'class': re.compile(r'ang_spalte_stadt.*')}}

                for key, F in filters.items():
                    filters[key] = [elem.a.text.strip() for elem in page.findAll('td', F)]

                page_ids = [listing['adid'] for listing in page.findAll('tr', {'adid': re.compile(r'.*')})]

                page_ids = [page_id for page_id, location in zip(page_ids, filters['location']) if '*' not in location]
                page_ids = [page_id for page_id, end_date in zip(page_ids, filters['end_date']) if end_date == '']

                if any(end_date == 'vermietet' for end_date in filters['end_date']):
                    break

                page_urls = [urllib.parse.urljoin(self.domain, relative_url) for relative_url in page_ids]

                page_urls = OrderedDict.fromkeys(page_urls)
                listings[url].update(page_urls)
                page_number += 1

                len(listings[url])

        return [url for listings in listings.values() for url in list(listings)[:min(n, len(listings))]]


    def has_captcha(self, soup):
        if soup.find('div', {'class': 'g-recaptcha'}):
            return True
        return False

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

        key_facts = soup.findAll('h2', {'class': 'headline headline-key-facts'})
        fact_types = soup.findAll('h3', {'class': 'headline headline-key-facts'})

        facts = {f(k): f(v) for k, v in zip(fact_types, key_facts)}
        facts = {k: v for k, v in facts.items() if k in [u'Zimmergr\xf6\xdfe', u'Gr\xf6\xdfe']}

        k, size = facts.popitem()

        return {'square_meters': int(size.replace(u'm\xb2', ''))}

    @metric
    def get_wg_info(self, soup):
        def clean(s): return re.sub('\s\s+', '', s.text)

        wg_details = soup.find('ul', {'class': 'ul-detailed-view-datasheet print_text_left'}).findAll('li')
        wg_details = [clean(li).lower() for li in wg_details]
        wg_details = ' '.join(wg_details)

        processing = {'WG_size': int,
                      'languages': lambda langauges: langauges.split(',')}

        regex = {'WG_size': re.compile(r'.*(?P<WG_size>[0-9])er wg.*'),
                 'languages': re.compile(r'.*sprach.*:(?P<languages>.*)')}

        res = {}
        for backref, pattern in regex.items():
            hits = pattern.search(wg_details)
            hits = set(hits.groups(backref)) if hits else []
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
        places = self.google_places(geo)

        return {'address': address, **geo, **places}

    @metric
    def get_listing_info(self, soup):
        divs = soup.findAll('div', {'class': 'col-xs-12'})
        h3s = map(lambda div: div.find('h3', {'class': 'headline headline-detailed-view-panel-title'}), divs)
        h3s = map(lambda h3: h3.text.strip() if h3 else h3, h3s)
        h3s = list(h3s)

        description = ''
        if u'Anzeigentext' in h3s:
            description = divs[h3s.index(u'Anzeigentext')]
            description = re.sub('\s\s+', '\n', description.text.strip())
        else:
            texts = soup.findAll('div', {'id': 'freitext_0'})
            if texts:
                description = '\n'.join([div.text for div in texts])

        if 'Angaben zum Objekt':
            details = divs[h3s.index(u'Angaben zum Objekt')]
            details = details.findAll('div', {'class': 'col-xs-6 col-sm-4 text-center print_text_left'})
            details = [re.sub('\s\s+', ' ', item.text).strip() for item in details]

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

        # TODO retrun None for desc if all ''
        return {'image_urls': urls, 'image_descriptions': desc}

    @metric
    def get_availability(self, soup):

        def is_date(s):
            try:
                dateutil.parser.parse(s)
                return True
            except ValueError:
                return False

        dates = soup.find('div', {'class':'col-sm-3'})
        dates = dates.find('p').findAll('b')
        dates = [b.text for b in dates]
        dates = filter(is_date, dates)

        dates = [dateutil.parser.parse(s, dayfirst=True) for s in dates]
        if len(dates) == 0:
            return {}
        if len(dates) == 1:
            return {'free_from': dates[0]}
        if len(dates) == 2:
            return {'free_from': dates[0], 'free_until': dates[1]}
