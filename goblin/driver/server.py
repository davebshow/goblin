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
    Class that wraps a connection pool. Currently doesn't do much, but may
    be useful in the future....

    :param pool.ConnectionPool pool:
    """

    def __init__(self, pool):
        self._pool = pool

    @property
    def pool(self):
        """
        Readonly property.

        :returns: :py:class:`ConnectionPool<goblin.driver.pool.ConnectionPool>`
        """
    async def close(self):
        """**coroutine** Close underlying connection pool."""
        await self._pool.close()

    async def connect(self):
        """**coroutine** Acquire a connection from the pool."""
        conn = await self._pool.acquire()
        return conn

    @classmethod
    async def open(cls, url, loop, *, ssl_context=None,
                   username='', password='', lang='gremlin-groovy',
                   response_timeout=None, max_conns=4, min_conns=1,
                   max_times_acquired=16, max_inflight=64):
        """
        **coroutine** Establish connection pool and host to Gremlin Server.

        :param str url: url for host Gremlin Server
        :param asyncio.BaseEventLoop loop:
        :param ssl.SSLContext ssl_context:
        :param str username: Username for database auth
        :param str password: Password for database auth
        :param str lang: Language used to submit scripts (optional)
            `gremlin-groovy` by default
        :param float response_timeout: (optional) `None` by default
        :param int max_conns: Maximum number of conns to a host
        :param int min_connsd: Minimum number of conns to a host
        :param int max_times_acquired: Maximum number of times a conn can be
            shared by multiple coroutines (clients)
        :param int max_inflight: Maximum number of unprocessed requests at any
            one time on the connection

        :returns: :py:class:`GremlinServer`
        """
        conn_pool = pool.ConnectionPool(
            url, loop, ssl_context=ssl_context, username=username,
            password=password, lang=lang, max_conns=max_conns,
            min_conns=min_conns, max_times_acquired=max_times_acquired,
            max_inflight=max_inflight, response_timeout=response_timeout)
        await conn_pool.init_pool()
        return cls(conn_pool)
