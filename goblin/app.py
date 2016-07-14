"""Goblin application class and class constructor"""
import collections
import logging

from goblin.gremlin_python import process
from goblin import driver, element, session


logger = logging.getLogger(__name__)


async def create_app(url, loop, **config):
    """Constructor function for :py:class:`Engine`. Connects to database
       and builds a dictionary of relevant vendor implmentation features"""
    features = {}
    async with await driver.GremlinServer.open(url, loop) as conn:
        # Propbably just use a parser to parse the whole feature list
        stream = await conn.submit(
            'graph.features().graph().supportsComputer()')
        msg = await stream.fetch_data()
        features['computer'] = msg
        stream = await conn.submit(
            'graph.features().graph().supportsTransactions()')
        msg = await stream.fetch_data()
        features['transactions'] = msg
        stream = await conn.submit(
            'graph.features().graph().supportsPersistence()')
        msg = await stream.fetch_data()
        features['persistence'] = msg
        stream = await conn.submit(
            'graph.features().graph().supportsConcurrentAccess()')
        msg = await stream.fetch_data()
        features['concurrent_access'] = msg
        stream = await conn.submit(
            'graph.features().graph().supportsThreadedTransactions()')
        msg = await stream.fetch_data()
        features['threaded_transactions'] = msg
    return Goblin(url, loop, features=features, **config)


# Main API classes
class Goblin:
    """Class used to encapsulate database connection configuration and generate
       database connections. Used as a factory to create :py:class:`Session`
       objects. More config coming soon."""
    DEFAULT_CONFIG = {
        'translator': process.GroovyTranslator('g')
    }

    def __init__(self, url, loop, *, features=None, **config):
        self._url = url
        self._loop = loop
        self._features = features
        self._config = self.DEFAULT_CONFIG
        self._config.update(config)
        self._vertices = collections.defaultdict(
            lambda: element.Vertex)
        self._edges = collections.defaultdict(lambda: element.Edge)

    @property
    def vertices(self):
        return self._vertices

    @property
    def edges(self):
        return self._edges

    @property
    def features(self):
        return self._features

    def from_file(filepath):
        pass

    def from_obj(obj):
        pass

    @property
    def translator(self):
        return self._config['translator']

    @property
    def url(self):
        return self._url

    def register(self, *elements):
        for element in elements:
            if element.__type__ == 'vertex':
                self._vertices[element.__label__] = element
            if element.__type__ == 'edge':
                self._edges[element.__label__] = element

    async def session(self, *, use_session=False):
        conn = await driver.GremlinServer.open(self.url, self._loop)
        return session.Session(self,
                               conn,
                               use_session=use_session)
