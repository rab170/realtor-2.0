import re

import json
import requests

import numpy as np

import holidays

from datetime import date
from datetime import time
from datetime import datetime
from datetime import timedelta

from collections import OrderedDict
from itertools import chain

from .utilities import coordinate_distance

class VBB:

    def __init__(self, server_url=None, accessId=None):

        self.baseurl = server_url
        self.accessId = accessId
        self.response_format = 'json'

    def build_uri(self, service):
        params = {'base_uri': self.baseurl, 'format': self.response_format,
                  'accessId': self.accessId, 'service': service}

        return '{base_uri}/{service}?accessId={accessId}&format={format}'.format(**params)

    def nearby_stops(self, origin, **params):
        location_params = self.get_location_params(origin)

        params = {**params, **location_params}
        param_str = '&'.join('{k}={v}'.format(k=k, v=v) for k, v in params.items())

        base = self.build_uri('location.nearbystops')
        request = '{base}&{params}'.format(base=base, params=param_str)
        res = requests.get(request)
        if res.ok:
            res = json.loads(res.text)

            stops = []
            for stop in res['stopLocationOrCoordLocation']:
                stop = stop['StopLocation']
                stop['distance'] = stop.pop('dist')
                stop['lng'] = stop.pop('lon')
                stops.append(stop)
            return sorted(stops, key=lambda stop: stop['distance'])
        return []

    def get_routes(self, origin, destination):
        # TODO rename route_options stuff more clearly

        route_options = []
        for mode, dist in {'bike': 1500, 'no bike': 0}.items():

            trip = self.trip(origin, destination, biking_distance=dist)

            if trip is not None:
                for route_option in trip['Trip']:
                    route_option = self.simplify_trip_json(route_option)

                    valid_route = bool(route_option is not None)
                    duplicate = bool(route_option in route_options)
                    if valid_route and not duplicate:
                        route_options.append(route_option)

        dist = coordinate_distance(origin, destination)

        modes = {'WALK': 4.5, 'BIKE': 15}
        estimates = [{'duration': int(dist * 1e-3 / (speed / 60)),
                      'bike_required': mode == 'BIKE',
                      'speed': speed,
                      'source': 'estimate',
                      'distance': int(dist),
                      'transit_modes': [mode]} for mode, speed in modes.items()]

        return route_options + estimates

    def trip(self, origin, destination, biking_distance=0, **params):

        datetime_params = self.get_datetime_params()
        bike_params = self.get_bike_params(biking_distance)

        origin_params = self.get_location_params(origin)
        destination_params = self.get_location_params(destination, location_type='dest')

        params = {**params, **origin_params, **bike_params, **datetime_params, **destination_params}

        param_str = '&'.join('{k}={v}'.format(k=k, v=v) for k, v in params.items())
        base = self.build_uri('trip')
        request = '{base}&{params}'.format(base=base, params=param_str)

        res = requests.get(request)

        if res.ok:
            return json.loads(res.text)
        return None

    def simplify_trip_json(self, trip):

        legs = self.parse_legs(trip['LegList']['Leg'])

        for leg in legs:
            if 'name' in leg:
                if leg['name'] == '' and 'type' in leg:
                    leg['name'] = leg['type']

                leg['name'] = leg['name'].strip()
            if 'duration' in leg:
                leg['duration'] = self.parse_time_string(leg['duration'])

        transit_modes = [leg['name'] if 'name' in leg else None for leg in legs]
        if transit_modes[0] != transit_modes[-1]:
            return None

        distances = [leg['dist'] if 'dist' in leg else None for leg in legs]
        durations = [leg['duration'] if 'duration' in leg else None for leg in legs]

        bike = any(mode == 'BIKE' for mode in transit_modes)

        stops = [(leg['Origin']['name'], leg['Destination']['name']) for leg in legs]
        stops = list(chain.from_iterable(stops))
        stops = list(OrderedDict.fromkeys(stops).keys())

        duration = self.parse_time_string(trip['duration'])

        return {'duration': duration, 'stops': stops, 'transit_modes': transit_modes,
                'distances': distances, 'durations': durations, 'bike_required': bike, 'source': 'vbb'}

    @staticmethod
    def parse_legs(legs):
        leg_keys = ['name', 'type', 'lat', 'lon', 'type', 'duration', 'dist']

        leg_data = []
        for leg in legs:
            leg_locations = {}
            for location in ['Origin', 'Destination']:
                leg_locations[location] = {k: v for k, v in leg[location].items() if k in leg_keys}

            leg_info = {k: v for k, v in leg.items() if k in leg_keys}
            leg_data.append({**leg_info, **leg_locations})
        return leg_data

    @staticmethod
    def parse_time_string(time_string):
        mins = 0
        hours = 0

        hour_search = re.search('(\d*)H', time_string)
        min_search = re.search('(\d*)M', time_string)

        if hour_search:
            hours = int(hour_search.group(1))
        if min_search:
            mins = int(min_search.group(1))

        return 60 * hours + mins

    @staticmethod
    def get_datetime_params():
        bundestland_feiertage = holidays.Germany(prov='BE')                     # Get Holidays in Berlin

        week = [date.today() + timedelta(days=n) for n in range(7)]

        weekend = np.array([day.weekday() >= 5 for day in week])                # for a fair comparison: Weekday.
        holiday = np.array([day in bundestland_feiertage for day in week])      # for a fair comparison: not a holiday

        offset = np.argmax((weekend | holiday) == False)

        travel_date = date.today() + timedelta(days=int(offset))
        travel_time = datetime.combine(travel_date, time(10, 30))               # for a fair comparison: 10 AM

        return {'date': travel_date.strftime("%Y-%m-%d"),   # Represented in the format YYYY-MM-DD
                'time': travel_time.strftime('%H:%M')}      # Represented in the format hh:mm[:ss] in 24h nomenclature

    @staticmethod
    def get_location_params(coord, location_type='origin'):

        location = {}
        s = '{location_type}Coord{coord_type}'

        coord = {'lat': coord['lat'], 'long': coord['lng']}

        for coord_type, coordinate in coord.items():
            key = s.format(location_type=location_type, coord_type=coord_type.capitalize())
            location[key] = float(coordinate)
        return location

    @staticmethod
    def get_bike_params(biking_distance):
        bike = {'destBike': '1,0,{dist}', 'originBike': '1,0,{dist}'}
        return {k: v.format(dist=biking_distance) for k, v in bike.items()}

