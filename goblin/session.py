"""Main OGM API classes and constructors"""
import collections
import logging

from goblin import mapper
from goblin import traversal
from goblin.driver import connection


logger = logging.getLogger(__name__)


class Session(connection.AbstractConnection):
    """Provides the main API for interacting with the database. Does not
       necessarily correpsond to a database session."""

    def __init__(self, engine, *, use_session=False):
        self._engine = engine
        self._loop = self._engine._loop
        self._use_session = False
        self._session = None
        self._traversal_factory = traversal.TraversalFactory(
            self, self.engine.translator, self._loop)
        self._pending = collections.deque()
        self._current = {}

    @property
    def engine(self):
        return self._engine

    @property
    def traversal_factory(self):
        return self._traversal_factory

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

    @property
    def g(self):
        """Returns a simple traversal source"""
        return self.traversal_factory.traversal().graph.traversal()

    def traversal(self, element_class):
        """Returns a traversal spawned from an element class"""
        label = element_class.__mapping__.label
        return self.traversal_factory.traversal(
            element_class=element_class).traversal()

    async def save(self, element):
        if element.__type__ == 'vertex':
            result = await self.save_vertex(element)
        elif element.__type__ == 'edge':
            result = await self.save_edge(element)
        else:
            raise Exception("Unknown element type")
        return result

    async def save_vertex(self, element):
        result = await self._save_element(
            element, self._check_vertex,
            self.traversal_factory.add_vertex,
            self.traversal_factory.update_vertex)
        self.current[result.id] = result
        return result

    async def save_edge(self, element):
        if not (hasattr(element, 'source') and hasattr(element, 'target')):
            raise Exception("Edges require source/target vetices")
        result = await self._save_element(
            element, self._check_edge,
            self.traversal_factory.add_edge,
            self.traversal_factory.update_edge)
        self.current[result.id] = result
        return result

    async def _save_element(self,
                            element,
                            check_func,
                            create_func,
                            update_func):
        if hasattr(element, 'id'):
            result = await check_func(element)
            if not result.data:
                element = await create_func(element)
            else:
                element = await update_func(element)
        else:
            element = await create_func(element)
        return element

    async def remove_vertex(self, element):
        result = await self.traversal_factory.remove_vertex(element)
        del self.current[element.id]
        return result

    async def remove_edge(self, element):
        result = await self.traversal_factory.remove_edge(element)
        del self.current[element.id]
        return result

    async def get_vertex(self, element):
        return await self.traversal_factory.get_vertex_by_id(element)

    async def get_edge(self, element):
        return await self.traversal_factory.get_edge_by_id(element)

    async def _check_vertex(self, element):
        """Used to check for existence, does not update session element"""
        traversal = self.g.V(element.id)
        stream = await self.submit(repr(traversal))
        return await stream.fetch_data()

    async def _check_edge(self, element):
        """Used to check for existence, does not update session element"""
        traversal = self.g.E(element.id)
        stream = await self.submit(repr(traversal))
        return await stream.fetch_data()


    async def submit(self,
                    gremlin,
                    *,
                    bindings=None,
                    lang='gremlin-groovy'):
        if self.engine._features['transactions'] and not self._use_session():
            gremlin = self._wrap_in_tx(gremlin)
        stream = await self.engine.submit(gremlin, bindings=bindings,
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
