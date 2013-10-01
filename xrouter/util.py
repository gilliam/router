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

from gevent.event import Event
import gevent


class EtcdWatcher(object):

    def __init__(self, client, prefix, handler_set, handler_delete,
                 index=None):
        self.client = client
        self.prefix = prefix
        self.handler_set = handler_set
        self.handler_delete = handler_delete
        self.index = index
        self._watcher = None
        self._stopped = Event()
        self._get = lambda f, n: self._store.get((f, n))

    def start(self):
        self._watcher = gevent.spawn(self._do_watch)

    def stop(self):
        self._stopped.set()

    def _do_watch(self):
        while not self._stopped.is_set():
            event = self.client.watch(self.prefix, index=self.index, timeout=5)
            if event is None:
                continue
            self._dispatch(event)
            self.index = self._next_index(event, self.index)

    def _next_index(self, event, index):
        if index is None:
            return event.index + 1
        if event.index >= index:
            return event.index + 1

    def _dispatch(self, event):
        if event.action == 'SET':
            self.handler_set(event)
        elif event.action == 'DELETE':
            self.handler_delete(event)
