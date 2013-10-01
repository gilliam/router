# Copyright 2013 Johan Rydberg.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from gevent import monkey
monkey.patch_all()

import logging
from optparse import OptionParser
import os

import etcd
from gevent import pywsgi
import requests

from .errors import NoRouteError
from .store import RouteStoreQuery
from .proxy import Proxy, is_hop_by_hop


class Router(object):
    """Simple router that receives a request, transforms it according
    to rules stored in C{storage} and then forwards the request.
    """

    def __init__(self, store):
        self.store = store
    
    def route(self, request):
        target = self._route(request)
        # FIXME: add query string
        print repr(request.body_file)
        return requests.Request(request.method, target,
                                headers=self._make_headers(request),
                                data=request.body_file_raw,
                                cookies=request.cookies)

    def _route(self, request):
        route, vars = self._find_matching_route(request)
        return route.target.format(**vars)

    def _make_headers(self, request):
        headers = dict((key, value)
                       for key, value in request.headers.items()
                       if not is_hop_by_hop(key))
        for (header, value) in [('X-Forwarded-For', request.remote_addr),
                                ('X-Forwarded-Host', request.host),
                                ('X-Forwarded-Proto', request.scheme),
                                ('X-Forwarded-Protocol', request.scheme)]:
            if header not in headers and value:
                headers[header] = value
        print "HEADERS", headers
        return headers

    def _find_matching_route(self, request):
        for route in self.store.index():
            match = route.match(request)
            if match is not None:
                return route, match
        raise NoRouteError()


def main():
    parser = OptionParser()
    parser.add_option("-p", "--port", dest="port", type=int,
                      default=80, help="listen port", metavar="PORT")
    (options, args) = parser.parse_args()

    format = '%(levelname)-8s %(name)s: %(message)s'
    logging.basicConfig(level=logging.DEBUG, format=format)

    formation = os.getenv('GILLIAM_FORMATION')
    store_client = etcd.Etcd(host='_store.%s.service' % (formation,))

    store_query = RouteStoreQuery(store_client)
    store_query.start()

    app = Proxy(requests.Session(), Router(store_query))

    logging.info("start accepting connections on %d" % (
            options.port,))
    pywsgi.WSGIServer(('', options.port), app).serve_forever()
