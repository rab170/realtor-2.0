#!/usr/bin/env python
from apartment_finder.data_collection.craigslist_rss import AptFeed

if __name__ == '__main__':

    # this forces the listing to have a picture AND square footage
    # consider changing to allow for no square footage...much harder
    # to extract bedroom and size metrics..but may lose some gems

    parser = AptFeed('', '', '~/proxies', 'apartment_listings','an0nym1ty' )
    parser.archive()
