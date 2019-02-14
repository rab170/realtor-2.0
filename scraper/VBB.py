import json
import requests

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
        params = {**params, **self.location(origin)}
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

    def trip(self, origin, destination, **params):
        params = {**params, **self.location(origin),
                  **self.location(destination, location_type='dest')}

        param_str = '&'.join('{k}={v}'.format(k=k, v=v) for k, v in params.items())
        base = self.build_uri('trip')
        request = '{base}&{params}'.format(base=base, params=param_str)
        res = requests.get(request)
        if res.ok:
            return json.loads(res.text)

    @staticmethod
    def location(coord, location_type='origin'):
        location = {}
        s = '{location_type}Coord{coord_type}'

        for coord_type, coordinate in coord.items():
            key = s.format(location_type=location_type, coord_type=coord_type.capitalize())
            location[key] = float(coordinate)
        return location