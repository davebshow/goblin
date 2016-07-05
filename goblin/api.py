"""Main OGM API classes and constructors"""
import collections
import logging

from goblin import gremlin_python
from goblin import mapper
from goblin import properties
from goblin import query
from goblin import gremlin_python_driver


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
    # Will use a driver here
    driver = gremlin_python_driver.Driver(url, loop)
    async with driver.get() as conn:
        # Propbably just use a parser to parse the whole feature list
        stream = conn.submit(
            'graph.features().graph().supportsComputer()')
        msg = await stream.fetch_data()
        features['computer'] = msg.data[0]
        stream = conn.submit(
            'graph.features().graph().supportsTransactions()')
        msg = await stream.fetch_data()
        features['transactions'] = msg.data[0]
        stream = conn.submit(
            'graph.features().graph().supportsPersistence()')
        msg = await stream.fetch_data()
        features['persistence'] = msg.data[0]
        stream = conn.submit(
            'graph.features().graph().supportsConcurrentAccess()')
        msg = await stream.fetch_data()
        features['concurrent_access'] = msg.data[0]
        stream = conn.submit(
            'graph.features().graph().supportsThreadedTransactions()')
        msg = await stream.fetch_data()
        features['threaded_transactions'] = msg.data[0]

    return Engine(url, loop, driver=driver, **features)


# Main API classes
class Engine:
    """Class used to encapsulate database connection configuration and generate
       database connections. Used as a factory to create :py:class:`Session`
       objects. More config coming soon."""

    def __init__(self, url, loop, *, driver=None, force_close=True, **features):
        self._url = url
        self._loop = loop
        self._force_close = force_close
        self._features = features
        self._translator = gremlin_python.GroovyTranslator('g')
        # This will be a driver
        if driver is None:
            driver = gremlin_python_driver.Driver(url, loop)
        self._driver = driver

    @property
    def translator(self):
        return self._translator

    @property
    def url(self):
        return self._url

    @property
    def driver(self):
        return self._driver

    def session(self, *, use_session=False):
        return Session(self, use_session=use_session)

    async def execute(self, query, *, bindings=None, session=None):
        conn = await self.driver.recycle()
        return conn.submit(query, bindings=bindings)

    async def close(self):
        await self.driver.close()
        self._driver = None


class Session:
    """Provides the main API for interacting with the database. Does not
       necessarily correpsond to a database session."""

    def __init__(self, engine, *, use_session=False):
        self._engine = engine
        self._use_session = False
        self._session = None
        self._g = gremlin_python.PythonGraphTraversalSource(
            self.engine.translator)
        self._pending = collections.deque()
        self._current = {}
        self._binding = 0

    @property
    def g(self):
        return self._g

    @property
    def engine(self):
        return self._engine

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
                                          self._get_vertex_by_id,
                                          self._create_vertex,
                                          self._update_vertex,
                                          mapper.map_vertex_to_ogm)
        self.current[result.id] = result
        return result

    async def save_edge(self, element):
        if not (hasattr(element, 'source') and hasattr(element, 'target')):
            raise Exception("Edges require source/target vetices")
        result = await self._save_element(element,
                                          self._get_edge_by_id,
                                          self._create_edge,
                                          self._update_edge,
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
            traversal = get_func(element.id)
            stream = await self._execute_traversal(traversal)
            result = await stream.fetch_data()
            if not result.data:
                traversal = create_func(element)
            else:
                traversal = update_func(element)
        else:
            traversal = create_func(element)
        stream = await self._execute_traversal(traversal)
        result = await stream.fetch_data()
        return mapper_func(result.data[0], element, element.__mapping__)

    async def delete_vertex(self, element):
        traversal = self.g.V(element.id).drop()
        result = await self._delete_element(element, traversal)
        return result

    async def delete_edge(self, element):
        traversal = self.g.E(element.id).drop()
        result = await self._delete_element(element, traversal)
        return result

    async def _delete_element(self, element, traversal):
        stream = await self._execute_traversal(traversal)
        result = await stream.fetch_data()
        del self.current[element.id]
        return result

    async def get_vertex(self, element):
        traversal = self._get_vertex_by_id(element.id)
        stream = await self._execute_traversal(traversal)
        result = await stream.fetch_data()
        if result.data:
            vertex = mapper.map_vertex_to_ogm(result.data[0], element,
                                              element.__mapping__)
            return vertex

    def _get_vertex_by_id(self, vid):
        return self.g.V(vid)

    async def get_edge(self, element):
        traversal = self._get_edge_by_id(element.id)
        stream = await self._execute_traversal(traversal)
        result = await stream.fetch_data()
        if result.data:
            edge = mapper.map_edge_to_ogm(result.data[0], element,
                                          element.__mapping__)
            return edge

    def _get_edge_by_id(self, eid):
        return self.g.E(eid)

    def _create_vertex(self, element):
        props = mapper.map_props_to_db(element, element.__mapping__)
        traversal = self.g.addV(element.__mapping__.label)
        return self._add_properties(traversal, props)

    def _create_edge(self, element):
        props = mapper.map_props_to_db(element, element.__mapping__)
        traversal = self.g.V(element.source.id)
        traversal = traversal.addE(element.__mapping__._label)
        traversal = traversal.to(self.g.V(element.target.id))
        return self._add_properties(traversal, props)

    def _update_vertex(self, element):
        props = mapper.map_props_to_db(element, element.__mapping__)
        traversal = self.g.V(element.id)
        return self._add_properties(traversal, props)

    def _update_edge(self, element):
        props = mapper.map_props_to_db(element, element.__mapping__)
        traversal = self.g.E(element.id)
        return self._add_properties(traversal, props)

    def _add_properties(self, traversal, props):
        for k, v in props:
            if v:
                traversal = traversal.property(
                    ('k' + str(self._binding), k),
                    ('v' + str(self._binding), v))
                self._binding += 1
        self._binding = 0
        return traversal

    async def _execute_traversal(self, traversal):
        script, bindings = query.parse_traversal(traversal)
        if self.engine._features['transactions'] and not self._use_session():
            script = self._wrap_in_tx(script)
        stream = await self.engine.execute(script, bindings=bindings,
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


# Graph elements
class ElementMeta(type):
    """Metaclass for graph elements. Responsible for creating the
       the :py:class:`mapper.Mapping` object and replacing user defined
       :py:class:`property.Property` with
       :py:class:`property.PropertyDescriptor`"""
    def __new__(cls, name, bases, namespace, **kwds):
        if bases:
            namespace['__type__'] = bases[0].__name__.lower()
        props = {}
        new_namespace = {}
        for k, v in namespace.items():
            if isinstance(v, properties.Property):
                props[k] = v
                data_type = v.data_type
                v = properties.PropertyDescriptor(k, data_type,
                                                  default=v._default)
            new_namespace[k] = v
        new_namespace['__mapping__'] = mapper.create_mapping(namespace,
                                                             props)
        logger.warning("Creating new Element class: {}".format(name))
        result = type.__new__(cls, name, bases, new_namespace)
        return result


class Vertex(metaclass=ElementMeta):
    """Base class for user defined Vertex classes"""
    pass


class Edge(metaclass=ElementMeta):
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
        raise ValueError("Cant")

    source = property(getsource, setsource, delsource)

    def gettarget(self):
        return self._target

    def settarget(self, val):
        assert isinstance(val, Vertex) or val is None
        self._target = val

    def deltarget(self):
        del self._target

    target = property(gettarget, settarget, deltarget)
