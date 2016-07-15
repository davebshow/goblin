"""Query API and helpers"""
import asyncio
import functools
import logging

from goblin import mapper
from goblin.driver import connection, graph


logger = logging.getLogger(__name__)


class TraversalResponse:

    def __init__(self, response_queue):
        self._queue = response_queue
        self._done = False

    async def __aiter__(self):
        return self

    async def __anext__(self):
        if self._done:
            return
        msg = await self._queue.get()
        if msg:
            return msg
        else:
            self._done = True
            raise StopAsyncIteration


# This is all until we figure out GLV integration...
class GoblinTraversal(graph.AsyncGraphTraversal):

    async def all(self):
        return await self.next()

    async def one_or_none(self):
        result = None
        async for msg in await self.next():
            result = msg
        return result


class TraversalFactory:
    """Helper that wraps a AsyncRemoteGraph"""
    def __init__(self, graph):
        self._graph = graph

    @property
    def graph(self):
        return self._graph

    def traversal(self, *, element_class=None):
        traversal = self.graph.traversal()
        if element_class:
            label = element_class.__mapping__.label
            traversal = self._graph.traversal()
            if element_class.__type__ == 'vertex':
                traversal = traversal.V()
            if element_class.__type__ == 'edge':
                traversal = traversal.E()
            traversal = traversal.hasLabel(label)
        return traversal

    def remove_vertex(self, element):
        return self.traversal().V(element.id).drop()

    def remove_edge(self, element):
        return self.traversal().E(element.id).drop()

    def get_vertex_by_id(self, element):
        return self.traversal().V(element.id)

    def get_edge_by_id(self, element):
        return self.traversal().E(element.id)

    def add_vertex(self, element):
        props = mapper.map_props_to_db(element, element.__mapping__)
        traversal = self.traversal().addV(element.__mapping__.label)
        return self._add_properties(traversal, props)

    def add_edge(self, element):
        props = mapper.map_props_to_db(element, element.__mapping__)
        traversal = self.traversal().V(element.source.id)
        traversal = traversal.addE(element.__mapping__._label)
        traversal = traversal.to(
            self.traversal().V(element.target.id))
        return self._add_properties(traversal, props)

    def _add_properties(self, traversal, props):
        binding = 0
        for k, v in props:
            if v:
                traversal = traversal.property(
                    ('k' + str(binding), k),
                    ('v' + str(binding), v))
                binding += 1
        return traversal
