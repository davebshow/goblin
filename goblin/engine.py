"""Main OGM API classes and constructors"""
import collections
import logging

from goblin.gremlin_python import process
from goblin import driver
from goblin import session


logger = logging.getLogger(__name__)


# Constructor API
async def create_engine(url,
                        loop,
                        maxsize=256,
                        force_close=False,
                        force_release=True):
    """Constructor function for :py:class:`Engine`. Connects to database
       and builds a dictionary of relevant vendor implmentation features"""
    features = {}
    # This will be some kind of manager client etc.
    conn = await driver.GremlinServer.open(url, loop)
    # Propbably just use a parser to parse the whole feature list
    stream = await conn.submit(
        'graph.features().graph().supportsComputer()')
    msg = await stream.fetch_data()
    features['computer'] = msg.data[0]
    stream = await conn.submit(
        'graph.features().graph().supportsTransactions()')
    msg = await stream.fetch_data()
    features['transactions'] = msg.data[0]
    stream = await conn.submit(
        'graph.features().graph().supportsPersistence()')
    msg = await stream.fetch_data()
    features['persistence'] = msg.data[0]
    stream = await conn.submit(
        'graph.features().graph().supportsConcurrentAccess()')
    msg = await stream.fetch_data()
    features['concurrent_access'] = msg.data[0]
    stream = await conn.submit(
        'graph.features().graph().supportsThreadedTransactions()')
    msg = await stream.fetch_data()
    features['threaded_transactions'] = msg.data[0]

    return Engine(url, conn, loop, **features)


# Main API classes
class Engine(driver.AbstractConnection):
    """Class used to encapsulate database connection configuration and generate
       database connections. Used as a factory to create :py:class:`Session`
       objects. More config coming soon."""

    def __init__(self, url, conn, loop, *, force_close=True, **features):
        self._url = url
        self._conn = conn
        self._loop = loop
        self._force_close = force_close
        self._features = features
        self._translator = process.GroovyTranslator('g')

    @property
    def translator(self):
        return self._translator

    @property
    def url(self):
        return self._url

    @property
    def conn(self):
        return self._conn

    def session(self, *, use_session=False):
        return session.Session(self, use_session=use_session)

    async def submit(self, query, *, bindings=None, session=None):
        return await self._conn.submit(query, bindings=bindings)

    async def close(self):
        await self.conn.close()
        self._conn = None
