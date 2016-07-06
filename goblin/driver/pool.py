"""Simple Async driver for the TinkerPop3 Gremlin Server"""
import asyncio
import collections
import logging

import aiohttp


logger = logging.getLogger(__name__)


class Pool:

    def __init__(self, url, loop, *, client_session=None):
        self._url = url
        self._loop = loop
        if not client_session:
            client_session = aiohttp.ClientSession(loop=self._loop)
        self._client_session = client_session
        self._queue = collections.deque()
        self._condition = asyncio.Condition(loop=loop)
        self._acquired = set()
        self._connecting = 0
        self._max_connections = 4

    @property
    def condition(self):
        return self._condition

    @property
    def max_connections(self):
        return self._max_connections

    @property
    def total_connections(self):
        return self._connecting + len(self._acquired) + len(self._queue)

    async def _get_new_connection(self, force_close, force_reclaim):
        if self.total_connections <= self._max_connections:
            self._connecting += 1
            try:
                ws = await self._client_session.ws_connect(self._url)
                conn = Connection(ws, self._loop, force_close=force_close,
                                  force_reclaim=force_reclaim, driver=self)
                return conn
            finally:
                self._connecting -= 1
        else:
            raise RuntimeError("To many connections, try recycling")

    async def acquire(self, *, force_close=False, force_reclaim=True):
        async with self.condition:
            while True:
                if self._queue:
                    while self._queue:
                        conn = self._queue.popleft()
                        if not conn.closed:
                            logger.info("Reusing connection: {}".format(conn))
                            self._acquired.add(conn)
                            return conn
                        else:
                            logger.debug(
                                "Discarded closed connection: {}".format(conn))
                elif self.total_connections < self.max_connections:
                    conn = await self._get_new_connection(force_close,
                                                          force_reclaim)
                    logger.info("Acquired new connection: {}".format(conn))
                    self._acquired.add(conn)
                    return conn
                else:
                    await self.condition.wait()

    async def release(self, conn):
        try:
            self._acquired.remove(conn)
        except:
            raise Exception("Unknown connection")
        if self.total_connections <= self.max_connections:
            if conn.closed:
                # conn has been closed
                logger.info(
                    "Released closed connection: {}".format(conn))
                conn = None
            else:
                self._queue.append(conn)
            await self._wakeup()
        else:
            if conn.driver is self:
                # hmmm
                await conn.close()

    async def _wakeup(self):
        async with self.condition:
            self.condition.notify()

    async def close(self):
        async with self.condition:
            waiters = []
            while self._queue:
                conn = self._queue.popleft()
                waiters.append(conn.close())
            await asyncio.gather(*waiters, loop=self._loop)
            await self._client_session.close()
            self._client_session = None
            self._closed = True
            logger.debug("Driver {} has been closed".format(self))
