import collections
import logging

from goblin.gremlin_python_driver import driver


logger = logging.getLogger(__name__)


def create_pool(url,
                loop,
                maxsize=256,
                force_close=False,
                force_release=True):
    return Pool(url, loop, maxsize=maxsize, force_close=force_close,
                force_release=force_release)


class Pool(object):
    """
    Pool of :py:class:`goblin.gremlin_python_driver.Connection` objects.
    :param str url: url for Gremlin Server.
    :param float timeout: timeout for establishing connection (optional).
        Values ``0`` or ``None`` mean no timeout
    :param str username: Username for SASL auth
    :param str password: Password for SASL auth
    :param gremlinclient.graph.GraphDatabase graph: The graph instances
        used to create connections
    :param int maxsize: Maximum number of connections.
    :param loop: event loop
    """
    def __init__(self, url, loop, maxsize=256, force_close=False,
                 force_release=True):
        self._graph = url
        self._loop = loop
        self._maxsize = maxsize
        self._force_close = force_close
        self._force_release = force_release
        self._pool = collections.deque()
        self._acquired = set()
        self._acquiring = 0
        self._closed = False
        self._driver = driver.Driver(url, loop)
        self._conn = None

    @property
    def freesize(self):
        """
        Number of free connections
        :returns: int
        """
        return len(self._pool)

    @property
    def size(self):
        """
        Total number of connections
        :returns: int
        """
        return len(self._acquired) + self._acquiring + self.freesize

    @property
    def maxsize(self):
        """
        Maximum number of connections
        :returns: in
        """
        return self._maxsize

    @property
    def driver(self):
        """
        Associated graph instance used for creating connections
        :returns: :py:class:`gremlinclient.graph.GraphDatabase`
        """
        return self._driver

    @property
    def pool(self):
        """
        Object that stores unused connections
        :returns: :py:class:`collections.deque`
        """
        return self._pool

    @property
    def closed(self):
        """
        Check if pool has been closed
        :returns: bool
        """
        return self._closed or self._graph is None

    def get(self):
        return AsyncPoolConnectionContextManager(self)

    async def acquire(self):
        """
        Acquire a connection from the Pool
        :returns: Future -
            :py:class:`asyncio.Future`, :py:class:`trollius.Future`, or
            :py:class:`tornado.concurrent.Future`
        """
        if self._pool:
            while self._pool:
                conn = self._pool.popleft()
                if not conn.closed:
                    logger.debug("Reusing connection: {}".format(conn))
                    self._acquired.add(conn)
                    break
                else:
                    logger.debug(
                        "Discarded closed connection: {}".format(conn))
        elif self.size < self.maxsize:
            self._acquiring += 1
            conn = await self.driver.connect(force_close=self._force_close,
                force_release=self._force_release, pool=self)
            self._acquiring -= 1
            self._acquired.add(conn)
            logger.debug(
                "Acquired new connection: {}".format(conn))

        return conn

    async def release(self, conn):
        """
        Release a connection back to the pool.
        :param gremlinclient.connection.Connection: The connection to be
            released
        """
        if self.size <= self.maxsize:
            if conn.closed:
                # conn has been closed
                logger.info(
                    "Released closed connection: {}".format(conn))
                self._acquired.remove(conn)
                conn = None
            else:
                self._pool.append(conn)
                self._acquired.remove(conn)
        else:
            await conn.close()

    async def close(self):
        """
        Close pool
        """
        while self.pool:
            conn = self.pool.popleft()
            await conn.close()
        await self.driver.close()
        self._driver = None
        self._closed = True
        logger.info(
            "Connection pool {} has been closed".format(self))
