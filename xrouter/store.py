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

import json

import etcd
from routes.route import Route as RoutesRoute

from .util import EtcdWatcher


class Route(object):
    __attributes__ = ('name', 'domain', 'path', 'target')

    def __init__(self, **kwargs):
        for attr in self.__attributes__:
            setattr(self, attr, None)
        self._update(kwargs)
        self._domain_route, self._path_route = None, None

    def _update(self, kwargs):
        for attr in self.__attributes__:
            if attr in kwargs:
                setattr(self, attr, kwargs[attr])

    def match(self, request):
        d = None
        if self.domain:
            if self._domain_route is None:
                self._domain_route = RoutesRoute(None, self.domain, _explicit=True)
                self._domain_route.makeregexp([])
            m = self._domain_route.match(request.host)
            if m is not False:
                d = m.copy()
        if self.path:
            if self._path_route is None:
                self._path_route = RoutesRoute(None, self.path, _explicit=True)
                self._path_route.makeregexp([])
            m = self._path_route.match(request.path)
            if m is not False:
                if d is None:
                    d = {}
                d.update(m)
        return d

    def to_json(self):
        """Return a python dict."""
        return dict((attr, getattr(self, attr))
                    for attr in self.__attributes__)


class _RouteStoreCommon(object):
    FACTORY = Route
    PREFIX = 'routes'

    def _make_key(self, route):
        return '%s/%s' % (self.PREFIX, route.name)

    def _split_key(self, key):
        _prefix, name = key.split('/')
        assert _prefix == self.PREFIX
        return name


class RouteStoreCommand(_RouteStoreCommon):
    """Interface against the route store that allows modifing
    commands.
    """

    def __init__(self, client):
        self.client = client

    def create(self, **kwargs):
        route = self.FACTORY(**kwargs)
        self.client.set(self._make_key(route), json.dumps(route.to_json()))
        return route

    def delete(self, route):
        """Delete the given route."""
        self.client.delete(self._make_key(route))


class RouteStoreQuery(_RouteStoreCommon):
    """Interface against the route store that allows querying."""

    def __init__(self, client):
        self.client = client
        self._store = {}
        self._watcher = EtcdWatcher(client, self.PREFIX,
                                    self._handle_set,
                                    self._handle_delete)
        self.get = self._store.get

    def start(self):
        """Start the route store by reading all state into memory.
        """
        self._store.clear()
        self._get_all_routes()
        self._watcher.start()

    def stop(self):
        self._watcher.stop()

    def index(self):
        """Return routes that belong to a service."""
        return self._store.itervalues()

    def _get_all_routes(self):
        try:
            keys_values = self.client.get_recursive(self.PREFIX)
        except etcd.EtcdError:
            keys_values = {}
        except ValueError:
            keys_values = {}
        for value in keys_values.itervalues():
            self._create(json.loads(value))

    def _handle_set(self, event):
        name = self._split_key(event.key)
        route = self.get(name)
        if route is not None:
            value = json.loads(event.value)
            route._update(**value)
        else:
            value = json.loads(event.value)
            self._create(value)

    def _handle_delete(self, event):
        name = self._split_key(event.key)
        route = self.get(name)
        if route is not None:
            self._delete(route)

    def _create(self, value):
        route = self.FACTORY(**value)
        self._store[route.name] = route
        return route

    def _delete(self, route):
        del self._store[route.name]
