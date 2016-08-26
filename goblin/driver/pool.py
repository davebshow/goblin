import asyncio
import collections

import aiohttp

from goblin.driver import connection


async def connect(url, loop, *, ssl_context=None, username='', password='',
                  lang='gremlin-groovy', aliases=None):
    connector = aiohttp.TCPConnector(ssl_context=ssl_context, loop=loop)
    client_session = aiohttp.ClientSession(loop=loop, connector=connector)
    ws = await client_session.ws_connect(url)
    return connection.Connection(url, ws, loop, client_session,
                                 aliases=aliases, lang=lang,
                                 username=username, password=password)


class PooledConnection:

    def __init__(self, conn, pool):
        self._conn = conn
        self._pool = pool
        self._times_acquired = 0

    @property
    def times_acquired(self):
        return self._times_acquired

    def increment_acquired(self):
        self._times_acquired += 1

    def decrement_acquired(self):
        self._times_acquired -= 1

    async def submit(self,
                     gremlin,
                     *,
                     bindings=None,
                     lang=None,
                     aliases=None,
                     session=None):
        return await self._conn.submit(gremlin, bindings=bindings, lang=lang,
                                       aliases=aliases, session=session)

    async def release_task(self, resp):
        await resp.done.wait()
        await self.release()

    async def release(self):
        await self._pool.release(self)

    async def close(self):
        # close pool?
        await self._conn.close()
        self._conn = None
        self._pool = None

    @property
    def closed(self):
        return self._conn.closed


class ConnectionPool:

    def __init__(self, url, loop, *, ssl_context=None, username='',
                 password='', lang='gremlin-groovy', max_conns=4,
                 max_times_acquired=8, aliases=None, **kwargs):
        self._url = url
        self._loop = loop
        self._ssl_context = ssl_context
        self._username = username
        self._password = password
        self._lang = lang
        self._max_conns = max_conns
        self._max_times_acquired = max_times_acquired
        self._condition = asyncio.Condition(loop=self._loop)
        self._lock = asyncio.Lock(loop=self._loop)
        self._available = collections.deque()
        self._acquired = collections.deque()
        self._aliases = aliases

    @property
    def url(self):
        return self._url

    async def release(self, conn):
        async with self._condition:
            conn.decrement_acquired()
            if not conn.times_acquired:
                self._acquired.remove(conn)
                self._available.append(conn)
            self._condition.notify()

    async def acquire(self):
        async with self._condition:
            while True:
                while self._available:
                    conn = self._available.popleft()
                    if not conn.closed:
                        conn.increment_acquired()
                        self._acquired.append(conn)
                        return conn
                if len(self._acquired) < self._max_conns:
                    conn = await self.get_connection()
                    conn.increment_acquired()
                    self._acquired.append(conn)
                    return conn
                else:
                    for x in range(len(self._acquired)):
                        conn = self._acquired.popleft()
                        if conn.times_acquired < self._max_times_acquired:
                            conn.increment_acquired()
                            self._aquired.append(conn)
                            return conn
                        self._acquired.append(conn)
                    else:
                        await self._condition.wait()

    async def close(self):
        waiters = []
        while self._available:
            conn = self._available.popleft()
            waiters.append(conn.close())
        while self._acquired:
            conn = self._acquired.popleft()
            waiters.append(conn.close())
        await asyncio.gather(*waiters)

    async def get_connection(self, username=None, password=None, lang=None,
                             aliases=None):
        """
        Open a connection to the Gremlin Server.

        :param str url: Database url
        :param asyncio.BaseEventLoop loop: Event loop implementation
        :param str username: Username for server auth
        :param str password: Password for server auth

        :returns: :py:class:`Connection<goblin.driver.connection.Connection>`
        """
        username = username or self._username
        password = password or self._password
        aliasess = aliases or self._aliases
        lang = lang or self._lang
        conn = await connect(self._url, self._loop,
                             ssl_context=self._ssl_context,
                             username=username, password=password, lang=lang,
                             aliases=aliases)
        return PooledConnection(conn, self)
