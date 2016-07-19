# Copyright 2016 ZEROFAIL
#
# This file is part of Goblin.
#
# Goblin is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Goblin is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with Goblin.  If not, see <http://www.gnu.org/licenses/>.

"""Query API and helpers"""

import asyncio
import functools
import logging

from goblin import mapper
from goblin.driver import connection, graph


logger = logging.getLogger(__name__)


class TraversalResponse:
    """Asynchronous iterator that encapsulates a traversal response queue"""
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
        """
        Get all results from traversal.

        :returns: :py:class:`TraversalResponse` object
        """
        return await self.next()

    async def one_or_none(self):
        """
        Get one or zero results from a traveral.

        :returns: :py:class:`Element<goblin.element.Element>` object
        """
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
        """
        Generate a traversal using a user defined element class as a
        starting point.

        :param goblin.element.Element element_class: An optional element
            class that will dictate the element type (vertex/edge) as well as
            the label for the traversal source

        :returns: :py:class:`GoblinTraversal`
        """
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
        """Convenience function for generating crud traversals."""
        return self.traversal().V(element.id).drop()

    def remove_edge(self, element):
        """Convenience function for generating crud traversals."""
        return self.traversal().E(element.id).drop()

    def get_vertex_by_id(self, element):
        """Convenience function for generating crud traversals."""
        return self.traversal().V(element.id)

    def get_edge_by_id(self, element):
        """Convenience function for generating crud traversals."""
        return self.traversal().E(element.id)

    def add_vertex(self, element):
        """Convenience function for generating crud traversals."""
        props = mapper.map_props_to_db(element, element.__mapping__)
        traversal = self.traversal().addV(element.__mapping__.label)
        return self._add_properties(traversal, props)

    def add_edge(self, element):
        """Convenience function for generating crud traversals."""
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
