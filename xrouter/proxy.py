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

from .errors import NoRouteError

from requests.exceptions import RequestException
from requests.utils import get_environ_proxies

from webob.dec import wsgify
from webob.exc import HTTPNotFound, HTTPBadGateway
from webob import Response


HOPPISH_HEADERS = frozenset([
    'connection', 'keep-alive', 'proxy-authenticate',
    'proxy-authorization', 'te', 'trailers', 'transfer-encoding',
    'upgrade', 'proxy-connection'
])


def is_hop_by_hop(header):
    """Returns C{True} if the given C{header} is hop by hop."""
    return header.lower() in HOPPISH_HEADERS


def capitalize_header(hdr):
    """Turn a lower-case header into a Nice-One."""
    return '-'.join([p.capitalize() for p in hdr.split('-')])


class Proxy(object):
    """A simple reverse proxy."""

    def __init__(self, requests, router):
        self.requests = requests
        self.router = router
        self.proxies = get_environ_proxies('') or {}

    def _handle_request(self, request):
        try:
            outgoing = self.router.route(request).prepare()
        except NoRouteError:
            raise HTTPNotFound()
        except Exception:
            raise
            raise HTTPBadGateway()
        else:
            upstream = self.requests.send(outgoing, stream=True,
                                          proxies=self.proxies)
            response = Response(status=upstream.status_code, headers={})
            print upstream.headers
            for header, value in upstream.headers.items():
                print "ADD HEADER", header
                if is_hop_by_hop(header):
                    continue
                response.headers.add(capitalize_header(header),
                                     value)
            clen = response.headers.get('Content-Length')
            response.app_iter = upstream.iter_content(4096)
            response.content_length = clen
            return response

    @wsgify
    def __call__(self, request):
        return self._handle_request(request)
