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

import asyncio
import collections

import aiohttp

from goblin.driver import connection


class PooledConnection:
    """
    Wrapper for :py:class:`Connection<goblin.driver.connection.Connection>`
    that helps manage tomfoolery associated with connection pooling.

    :param goblin.driver.connection.Connection conn:
    :param goblin.driver.pool.ConnectionPool pool:
    """
    def __init__(self, conn, pool):
        self._conn = conn
        self._pool = pool
        self._times_acquired = 0

    @property
    def times_acquired(self):
        """
        Readonly property.

        :returns: int
        """
        return self._times_acquired

    def increment_acquired(self):
        """Increment times acquired attribute by 1"""
        self._times_acquired += 1

    def decrement_acquired(self):
        """Decrement times acquired attribute by 1"""
        self._times_acquired -= 1

    async def submit(self,
                     *,
                     processor='',
                     op='eval',
                     **args):
        """
        **coroutine** Submit a script and bindings to the Gremlin Server

        :param str processor: Gremlin Server processor argument
        :param str op: Gremlin Server op argument
        :param args: Keyword arguments for Gremlin Server. Depend on processor
            and op.

        :returns: :py:class:`Response` object
        """
        return await self._conn.submit(processor=processor, op=op, **args)

    async def release_task(self, resp):
        await resp.done.wait()
        self.release()

    def release(self):
        self._pool.release(self)

    async def close(self):
        """Close underlying connection"""
        await self._conn.close()
        self._conn = None
        self._pool = None

    @property
    def closed(self):
        """
        Readonly property.

        :returns: bool
        """
        return self._conn.closed


class ConnectionPool:
    """
    A pool of connections to a Gremlin Server host.

    :param str url: url for host Gremlin Server
    :param asyncio.BaseEventLoop loop:
    :param ssl.SSLContext ssl_context:
    :param str username: Username for database auth
    :param str password: Password for database auth
    :param float response_timeout: (optional) `None` by default
    :param int max_conns: Maximum number of conns to a host
    :param int min_connsd: Minimum number of conns to a host
    :param int max_times_acquired: Maximum number of times a conn can be
        shared by multiple coroutines (clients)
    :param int max_inflight: Maximum number of unprocessed requests at any
        one time on the connection
    """

    def __init__(self, url, loop, ssl_context, username, password, max_conns,
                 min_conns, max_times_acquired, max_inflight, response_timeout,
                 message_serializer):
        self._url = url
        self._loop = loop
        self._ssl_context = ssl_context
        self._username = username
        self._password = password
        self._max_conns = max_conns
        self._min_conns = min_conns
        self._max_times_acquired = max_times_acquired
        self._max_inflight = max_inflight
        self._response_timeout = response_timeout
        self._message_serializer = message_serializer
        self._condition = asyncio.Condition(loop=self._loop)
        self._available = collections.deque()
        self._acquired = collections.deque()

    @property
    def url(self):
        """
        Readonly property.

        :returns: str
        """
        return self._url

    async def init_pool(self):
        """**coroutine** Open minumum number of connections to host"""
        for i in range(self._min_conns):
            conn = await self._get_connection(self._username,
                                              self._password,
                                              self._max_inflight,
                                              self._response_timeout,
                                              self._message_serializer)
            self._available.append(conn)

    def release(self, conn):
        """
        Release connection back to pool after use.

        :param PooledConnection conn:
        """
        if conn.closed:
            self._acquired.remove(conn)
        else:
            conn.decrement_acquired()
            if not conn.times_acquired:
                self._acquired.remove(conn)
                self._available.append(conn)
        self._loop.create_task(self._notify())

    async def _notify(self):
        async with self._condition:
            self._condition.notify()

    async def acquire(self, username=None, password=None, max_inflight=None,
                      response_timeout=None, message_serializer=None):
        """**coroutine** Acquire a new connection from the pool."""
        username = username or self._username
        password = password or self._password
        response_timeout = response_timeout or self._response_timeout
        max_inflight = max_inflight or self._max_inflight
        message_serializer = message_serializer or self._message_serializer
        async with self._condition:
            while True:
                while self._available:
                    conn = self._available.popleft()
                    if not conn.closed:
                        conn.increment_acquired()
                        self._acquired.append(conn)
                        return conn
                if len(self._acquired) < self._max_conns:
                    conn = await self._get_connection(username, password,
                                                      max_inflight,
                                                      response_timeout,
                                                      message_serializer)
                    conn.increment_acquired()
                    self._acquired.append(conn)
                    return conn
                else:
                    for x in range(len(self._acquired)):
                        conn = self._acquired.popleft()
                        if conn.times_acquired < self._max_times_acquired:
                            conn.increment_acquired()
                            self._acquired.append(conn)
                            return conn
                        self._acquired.append(conn)
                    else:
                        await self._condition.wait()

    async def close(self):
        """**coroutine** Close connection pool."""
        waiters = []
        while self._available:
            conn = self._available.popleft()
            waiters.append(conn.close())
        while self._acquired:
            conn = self._acquired.popleft()
            waiters.append(conn.close())
        await asyncio.gather(*waiters)

    async def _get_connection(self, username, password, max_inflight,
                              response_timeout, message_serializer):
        conn = await connection.Connection.open(
            self._url, self._loop, ssl_context=self._ssl_context,
            username=username, password=password,
            response_timeout=response_timeout,
            message_serializer=message_serializer)
        conn = PooledConnection(conn, self)
        return conn
