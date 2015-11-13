import os
import urllib2
from itertools import cycle

class ProxyHandler(object):

    def __init__(self, proxy_list_path):
        self.N = -1
        if os.path.exists(proxy_list_path):
            with open(proxy_list_path) as f:
                proxies = [ (l.split()[0], l.split()[1]) for l in f.readlines() ]
            self.proxies = [ self.get_opener(ip, port) for ip, port in proxies ]
            self.proxy_cycle = cycle(self.proxies)
            self.N = len(self.proxies)

    def get_proxy(self):
        if self.N >= 0:
            return self.proxy_cycle.next()
        return None

    def get_proxy_rand(self):
        if self.N >= 0:
            index = random.randint(0, self.N - 1)
            return self.proxies[index]
        return None

    def get_opener(self, ip, port, auth=None):
        proxy_string = 'http://{0}:{1}'.format(ip, port)
        proxy = urllib2.ProxyHandler({'http':proxy_string})
        opener = urllib2.build_opener(proxy)
        return opener

def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise
