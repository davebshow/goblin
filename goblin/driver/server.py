# Copyright 2016 ZEROFAIL
#
# This file is part of Goblin.
#
# Goblin is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Goblin is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with Goblin.  If not, see <http://www.gnu.org/licenses/>.

from goblin.driver import pool


class GremlinServer:
    """
    Factory class that generates connections to the Gremlin Server. Do
    not instantiate directly, instead use :py:meth:GremlinServer.open
    """

    def __init__(self, pool, *, ssl_context=None,
                 username='', password='', lang='gremlin-groovy',
                 traversal_source=None):
        self._pool = pool
        self._url = self._pool.url
        self._loop = self._pool._loop
        self._ssl_context = ssl_context
        self._username = username
        self._password = password
        self._lang = lang
        self._traversal_source = traversal_source

    async def close(self):
        await self._pool.close()

    async def connect(self):
        # This will use pool eventually
        conn = await self._pool.acquire()
        return conn

    @classmethod
    async def open(cls, url, loop, *, ssl_context=None,
                   username='', password='', lang='gremlin-groovy',
                   traversal_source=None, max_conns=4, min_conns=1,
                   max_times_acquired=16, max_inflight=64,
                   response_timeout=None):

        conn_pool = pool.ConnectionPool(
            url, loop, ssl_context=ssl_context, username=username,
            password=password, lang=lang, traversal_source=traversal_source,
            max_conns=max_conns, min_conns=min_conns,
            max_times_acquired=max_times_acquired, max_inflight=max_inflight,
            response_timeout=response_timeout)
        await conn_pool.init_pool()
        return cls(conn_pool, ssl_context=ssl_context, username=username,
                   password=password, lang=lang, traversal_source=traversal_source)
