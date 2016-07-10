"""Main OGM API classes and constructors"""
import collections
import logging

from goblin import mapper
from goblin import query


logger = logging.getLogger(__name__)


class Session:
    """Provides the main API for interacting with the database. Does not
       necessarily correpsond to a database session."""

    def __init__(self, engine, *, use_session=False):
        self._engine = engine
        self._loop = self._engine._loop
        self._use_session = False
        self._session = None
        self._query = query.Query(self, self.engine.translator, self._loop)
        self._pending = collections.deque()
        self._current = {}

    @property
    def engine(self):
        return self._engine

    @property
    def query(self):
        return self._query

    @property
    def current(self):
        return self._current

    def add(self, *elements):
        for elem in elements:
            self._pending.append(elem)

    async def flush(self):
        while self._pending:
            elem = self._pending.popleft()
            await self.save(elem)

    def traversal(self, element_class):
        return self.query.traversal(element_class)

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
                                          self.query.get_vertex_by_id,
                                          self.query.add_vertex,
                                          self.query.update_vertex)
        self.current[result.id] = result
        return result

    async def save_edge(self, element):
        if not (hasattr(element, 'source') and hasattr(element, 'target')):
            raise Exception("Edges require source/target vetices")
        result = await self._save_element(element,
                                          self.query.get_edge_by_id,
                                          self.query.add_edge,
                                          self.query.update_edge)
        self.current[result.id] = result
        return result

    async def _save_element(self,
                            element,
                            get_func,
                            create_func,
                            update_func):
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
        return element.__mapping__.mapper_func(result.data[0], element)

    async def remove_vertex(self, element):
        traversal = self.query.remove_vertex(element)
        result = await self._remove_element(element, traversal)
        return result

    async def remove_edge(self, element):
        traversal = self.query.remove_edge(element)
        result = await self._remove_element(element, traversal)
        return result

    async def _remove_element(self, element, traversal):
        stream = await self.execute_traversal(traversal)
        result = await stream.fetch_data()
        del self.current[element.id]
        return result

    async def get_vertex(self, element):
        traversal = self.query.get_vertex_by_id(element)
        stream = await self.execute_traversal(traversal)
        result = await stream.fetch_data()
        if result.data:
            vertex = element.__mapping__.mapper_func(result.data[0], element)
            return vertex

    async def get_edge(self, element):
        traversal = self.query.get_edge_by_id(element)
        stream = await self.execute_traversal(traversal)
        result = await stream.fetch_data()
        if result.data:
            vertex = element.__mapping__.mapper_func(result.data[0], element)
            return vertex

    async def execute_traversal(self, traversal):
        # Move parsing to query
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
