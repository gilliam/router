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

from functools import partial
import logging
from optparse import OptionParser
import os

import etcd
from gevent import pywsgi
from routes.middleware import RoutesMiddleware
from routes import Mapper
from webob.dec import wsgify
from webob.exc import HTTPNotFound, HTTPBadRequest
from webob import Response

from . import store


def _collection(request, items, url, build, **links):
    """Convenience function for handing a collection request (aka
    'index').

    @param request: The HTTP request.
    @param items: Something that can be sliced and that will be fed
        into the C{build} function to generate a JSON representation
        of the item.
    @param url: a callable that returns a URL to the collection, and
        that also accepts keyword argments that will become query
        parameters to the URL.
    @param build: a callable that takes a single parameter, the item,
        and returns a python C{dict} that is the item representation.
        
    @param links: Additional links for the representation.
    """
    offset = int(request.params.get('offset', 0))
    page_size = int(request.params.get('page_size', 10))
    items = list(items[offset:offset + page_size])
    links['self'] = url(offset=offset, page_size=page_size)
    if offset > 0:
        links['prev'] = url(offset=offset - page_size,
                            page_size=page_size)
    if len(items) == page_size:
        links['next'] = url(offset=offset + page_size,
                            page_size=page_size)

    return Response(json={'items': [build(item) for item in items],
                          'links': links}, status=200)


class _BaseResource(object):
    """Base resource that do not allow anything."""

    def _check_not_found(self, item):
        if item is None:
            raise HTTPNotFound()

    def _assert_request_content(self, request, *fields):
        if not request.content_length:
            raise HTTPBadRequest()
        if request.json is None:
            raise HTTPBadRequest()
        data = request.json
        for field in fields:
            if not field in data:
                raise HTTPBadRequest()
        return data


class RouteResource(_BaseResource):
    """The routes resource."""

    def __init__(self, command, query):
        self.command = command
        self.query = query

    def _build(self, data):
        data = data.to_json()
        data.update({'kind': 'gilliam#route'})
        return data

    def index(self, request, url):
        items = list(self.query.index())
        return _collection(request, items, partial(url, 'routes'),
                           self._build)

    def show(self, request, url, route):
        data = self.store.get(route)
        self._check_not_found(data)
        return Response(json=self._build(data), status=200)

    def create(self, request, url):
        params = self._assert_request_content(request, 'name',
                                              'domain', 'path',
                                              'target')
        route = self.command.create(**params)
        response = Response(json=self._build(route), status=201)
        response.headers.add('Location', url('route', route=route.name))
        return response

    def delete(self, request, url, route):
        route = self.query.get(route)
        self._check_not_found(route)
        self.command.delete(route)
        return Response(status=204)


class API(object):
    """Our REST API WSGI application."""

    def __init__(self, log):
        self.log = log
        self.mapper = Mapper()
        self.controllers = {}
        
        self.mapper.collection("routes", "route",
            path_prefix="/route", controller="route",
            collection_actions=['index', 'create'],
            member_actions=['show', 'delete'],
            member_prefix="/{route}", formatted=False)

    def add(self, name, controller):
        self.controllers[name] = controller

    def create_app(self):
        return RoutesMiddleware(
            self, self.mapper, use_method_override=False, singleton=False)

    @wsgify
    def __call__(self, request):
        # handle incoming call.  depends on the routes middleware.
        url, match = request.environ['wsgiorg.routing_args']
        if match is None or 'controller' not in match:
            raise HTTPNotFound()
        resource = self.controllers[match.pop('controller')]
        action = match.pop('action')
        return getattr(resource, action)(request, url, **match)


def main():
    parser = OptionParser()
    parser.add_option("-p", "--port", dest="port", type=int,
                      default=80, help="listen port",
                      metavar="PORT")
    (options, args) = parser.parse_args()

    format = '%(levelname)-8s %(name)s: %(message)s'
    logging.basicConfig(level=logging.DEBUG, format=format)

    formation = os.getenv('GILLIAM_FORMATION')
    store_client = etcd.Etcd(host='_store.%s.service' % (formation,))

    store_command = store.RouteStoreCommand(store_client)
    store_query = store.RouteStoreQuery(store_client)
    store_query.start()

    api = API(logging.getLogger('api'))
    api.add('route', RouteResource(store_command, store_query))

    pywsgi.WSGIServer(('', options.port), api.create_app()).serve_forever()
