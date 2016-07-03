import collections

from goblin import gremlin_python
from goblin import mapper
from goblin import properties
from goblin import query
from goblin.gremlin_python_driver import driver


# Constructor API
async def create_engine(url, loop):
    features = {}
    # Will use a pool here
    async with driver.create_connection(url, loop) as conn:
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

    return Engine(url, loop, **features)


# Main API classes
class Engine:

    def __init__(self, url, loop, **features):
        self._url = url
        self._loop = loop
        self._features = features
        self._translator = gremlin_python.GroovyTranslator('g')
        self._g = gremlin_python.PythonGraphTraversalSource(self._translator)
        # This will be a pool
        self._driver = driver.Driver(self._url, self._loop)

    def session(self):
        return Session(self)

    @property
    def g(self):
        return self._g

    @property
    def url(self):
        return url

    async def execute(self, query, *, bindings=None):
        conn = await self._driver.connect()
        return conn.submit(query, bindings=bindings)

    async def close(self):
        await self._driver.close()
        self._driver = None


class Session:

    def __init__(self, engine):
        self._engine = engine
        self._pending = collections.deque()
        self._current = {}
        self._binding = 0

    @property
    def engine(self):
        return self._engine

    def query(self, element_class):
        return query.Query(self, element_class)

    def add(self, *elements):
        for elem in elements:
            self._pending.append(elem)

    async def flush(self):
        # Could optionally use sessions/transactions here
        # Need some optional kwargs etc...
        while self._pending:
            elem = self._pending.popleft()
            result = await self.save_element(elem)
            self._current[result.id] = result

    async def save_element(self, element):
        if element.__type__ == 'vertex':
            result = await self.save_vertex(element)
        elif element.__type__ == 'edge':
            result = await self.save_edge(element)
        else:
            result = None
        return result

    async def save_vertex(self, element):
        if hasattr(element, 'id'):
            # Something like
            # if self._current.get(element.id):
            #     old = self._current[element.id]
            #     element = merge_elements(old, element)
            script, bindings = self._get_vertex_by_id(element)
            stream = await self.engine.execute(script, bindings=bindings)
            result = await stream.fetch_data()
            await stream.close()
            if not result.data:
                script, bindings = self._create_vertex(element)
            else:
                script, bindings = self._update_vertex(element)
        else:
            script, bindings = self._create_vertex(element)
        stream = await self.engine.execute(script, bindings=bindings)
        result = await stream.fetch_data()
        # Will just release the conn back to pool here
        await stream.close()
        return mapper.map_vertex_to_ogm(result.data[0], element,
                                         element.__mapping__)

    def _create_vertex(self, element):
        props = mapper.map_props_to_db(element, element.__mapping__)
        traversal = self.engine.g.addV(element.__mapping__.label)
        for k, v in props:
            traversal = traversal.property(
                ('k' + str(self._binding), k), ('v' + str(self._binding), v))
            self._binding += 1
        self._binding = 0
        script = traversal.translator.traversal_script
        bindings = traversal.bindings
        return script, bindings

    def _update_vertex(self, element):
        raise NotImplementedError

    def _get_vertex_by_id(self, element):
        traversal = self.engine.g.V(element.id)
        script = traversal.translator.traversal_script
        bindings = traversal.bindings
        return result, bindings

    async def save_edge(self, element):
        if not (element.source and element.target):
            raise Exception("Edges require source/target vetices")
        if hasattr(element, 'id'):
            # Something like
            # if self._current.get(element.id):
            #     old = self._current[element.id]
            #     element = merge_elements(old, element)
            script, bindings = self._get_edge_by_id(element)
            stream = await self.engine.execute(script, bindings=bindings)
            result = await stream.fetch_data()
            await stream.close()
            if not result.data:
                script, bindings = self._create_edge(element)
            else:
                script, bindings = self._update_edge(element)
        else:
            script, bindings = self._create_edge(element)
        stream = await self.engine.execute(script, bindings=bindings)
        result = await stream.fetch_data()
        # Will just release the conn back to pool here
        await stream.close()
        return mapper.map_edge_to_ogm(result.data[0], element,
                                       element.__mapping__)

    def _create_edge(self, element):
        props = mapper.map_props_to_db(element, element.__mapping__)
        traversal = self.engine.g.V(element.source.id)
        traversal = traversal.addE(element.__mapping__._label)
        traversal = traversal.to(self.engine.g.V(element.target.id))
        for k, v in props:
            traversal = traversal.property(
                ('k' + str(self._binding), k), ('v' + str(self._binding), v))
            self._binding += 1
        self._binding = 0
        script = traversal.translator.traversal_script
        bindings = traversal.bindings
        return script, bindings

    def _update_edge(self, element):
        raise NotImplementedError

    def _get_edge_by_id(self, element):
        t = self.engine.g.E(element.id)
        script = t.translator.traversal_script
        bindings = t.bindings
        return result, bindings

    async def commit(self):
        await self.flush()

    async def rollback(self):
        raise NotImplementedError


# Graph elements
class ElementMeta(type):

    def __new__(cls, name, bases, namespace, **kwds):
        if bases:
            namespace['__type__'] = bases[0].__name__.lower()
        props = {}
        new_namespace = {}
        for k, v in namespace.items():
            if isinstance(v, properties.Property):
                props[k] = v
                v = properties.PropertyDescriptor(v.data_type)
            new_namespace[k] = v
        new_namespace['__mapping__'] = mapper.create_mapping(namespace,
                                                              props)
        result = type.__new__(cls, name, bases, new_namespace)
        return result


class Vertex(metaclass=ElementMeta):
    pass


class Edge(metaclass=ElementMeta):

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
