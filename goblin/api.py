"""Main OGM API classes and constructors"""
import collections
import logging

from goblin import gremlin_python
from goblin import driver
from goblin import mapper
from goblin import meta
from goblin import traversal
from goblin import query


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
        self._translator = gremlin_python.GroovyTranslator('g')

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
        return Session(self, use_session=use_session)

    async def submit(self, query, *, bindings=None, session=None):
        return await self._conn.submit(query, bindings=bindings)

    async def close(self):
        await self.conn.close()
        self._conn = None


class Session:
    """Provides the main API for interacting with the database. Does not
       necessarily correpsond to a database session."""

    def __init__(self, engine, *, use_session=False):
        self._engine = engine
        self._loop = self._engine._loop
        self._use_session = False
        self._session = None
        self._traversal = traversal.TraversalSource(self.engine.translator)
        self._pending = collections.deque()
        self._current = {}

    @property
    def engine(self):
        return self._engine

    @property
    def traversal(self):
        return self._traversal

    @property
    def current(self):
        return self._current

    def query(self, element_class):
        return query.Query(self, element_class)

    def add(self, *elements):
        for elem in elements:
            self._pending.append(elem)

    async def flush(self):
        while self._pending:
            elem = self._pending.popleft()
            await self.save(elem)

    async def save(self, element):
        if element.__type__ == 'vertex':
            result = await self.save_vertex(element)
        elif element.__type__ == 'edge':
            result = await self.save_edge(element)
        else:
            raise Exception("Unknown element type")
        return result

    async def save_vertex(self, element):
        result = await self._save_element(element,
                                          self.traversal.get_vertex_by_id,
                                          self.traversal.add_vertex,
                                          self.traversal.update_vertex,
                                          mapper.map_vertex_to_ogm)
        self.current[result.id] = result
        return result

    async def save_edge(self, element):
        if not (hasattr(element, 'source') and hasattr(element, 'target')):
            raise Exception("Edges require source/target vetices")
        result = await self._save_element(element,
                                          self.traversal.get_edge_by_id,
                                          self.traversal.add_edge,
                                          self.traversal.update_edge,
                                          mapper.map_edge_to_ogm)
        self.current[result.id] = result
        return result

    async def _save_element(self,
                            element,
                            get_func,
                            create_func,
                            update_func,
                            mapper_func):
        if hasattr(element, 'id'):
            traversal = get_func(element)
            stream = await self.execute_traversal(traversal)
            result = await stream.fetch_data()
            if not result.data:
                traversal = create_func(element)
            else:
                traversal = update_func(element)
        else:
            traversal = create_func(element)
        stream = await self.execute_traversal(traversal)
        result = await stream.fetch_data()
        return mapper_func(result.data[0], element, element.__mapping__)

    async def remove_vertex(self, element):
        traversal = self.traversal.remove_vertex(element)
        result = await self._remove_element(element, traversal)
        return result

    async def remove_edge(self, element):
        traversal = self.traversal.remove_edge(element)
        result = await self._remove_element(element, traversal)
        return result

    async def _remove_element(self, element, traversal):
        stream = await self.execute_traversal(traversal)
        result = await stream.fetch_data()
        del self.current[element.id]
        return result

    async def get_vertex(self, element):
        traversal = self.traversal.get_vertex_by_id(element)
        stream = await self.execute_traversal(traversal)
        result = await stream.fetch_data()
        if result.data:
            vertex = mapper.map_vertex_to_ogm(result.data[0], element,
                                              element.__mapping__)
            return vertex

    async def get_edge(self, element):
        traversal = self.traversal.get_edge_by_id(element)
        stream = await self.execute_traversal(traversal)
        result = await stream.fetch_data()
        if result.data:
            vertex = mapper.map_edge_to_ogm(result.data[0], element,
                                              element.__mapping__)
            return vertex

    async def execute_traversal(self, traversal):
        script, bindings = query.parse_traversal(traversal)
        if self.engine._features['transactions'] and not self._use_session():
            script = self._wrap_in_tx(script)
        stream = await self.engine.submit(script, bindings=bindings,
                                          session=self._session)
        return stream

    def _wrap_in_tx(self):
        raise NotImplementedError

    def tx(self):
        raise NotImplementedError

    async def commit(self):
        await self.flush()
        if self.engine._features['transactions'] and self._use_session():
            await self.tx()
        raise NotImplementedError

    async def rollback(self):
        raise NotImplementedError


class Vertex(metaclass=meta.ElementMeta):
    """Base class for user defined Vertex classes"""
    pass


class Edge(metaclass=meta.ElementMeta):
    """Base class for user defined Edge classes"""

    def __init__(self, source=None, target=None):
        if source:
            self._source = source
        if target:
            self._target = target

    def getsource(self):
        return self._source

    def setsource(self, val):
        assert isinstance(val, Vertex) or val is None
        self._source = val

    def delsource(self):
        del self._source

    source = property(getsource, setsource, delsource)

    def gettarget(self):
        return self._target

    def settarget(self, val):
        assert isinstance(val, Vertex) or val is None
        self._target = val

    def deltarget(self):
        del self._target

    target = property(gettarget, settarget, deltarget)


class VertexProperty(metaclass=meta.ElementMeta):

    __data_type__ = None

    def __init__(self, value):
        self._value = value

    @property
    def value(self):
        return self._value

    def __repr__(self):
        return '<{}(type={}, value={})'.format(self.__class__.__name__,
                                               self.__data_type__, self.value)
