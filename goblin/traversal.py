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

from goblin import cardinality, element, mapper
from goblin.driver import connection, graph
from gremlin_python import process


logger = logging.getLogger(__name__)


def bindprop(element_class, ogm_name, val, *, binding=None):
    """
    Helper function for binding ogm properties/values to corresponding db
    properties/values for traversals.

    :param goblin.element.Element element_class: User defined element class
    :param str ogm_name: Name of property as defined in the ogm
    :param val: The property value
    :param str binding: The binding for val (optional)

    :returns: tuple object ('db_property_name', ('binding(if passed)', val))
    """
    db_name = getattr(element_class, ogm_name, ogm_name)
    _, data_type = element_class.__mapping__.ogm_properties[ogm_name]
    val = data_type.to_db(val)
    if binding:
        val = (binding, val)
    return db_name, val


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

    def remove_vertex(self, elem):
        """Convenience function for generating crud traversals."""
        return self.traversal().V(elem.id).drop()

    def remove_edge(self, elem):
        """Convenience function for generating crud traversals."""
        return self.traversal().E(elem.id).drop()

    def get_vertex_by_id(self, elem):
        """Convenience function for generating crud traversals."""
        return self.traversal().V(elem.id)

    def get_edge_by_id(self, elem):
        """Convenience function for generating crud traversals."""
        return self.traversal().E(elem.id)

    def add_vertex(self, elem):
        """Convenience function for generating crud traversals."""
        props = mapper.map_props_to_db(elem, elem.__mapping__)
        # vert_props = mapper.map_props_to_db
        traversal = self.traversal().addV(elem.__mapping__.label)
        traversal, _ = self.add_properties(traversal, props)
        # traversal, _ = self.add_vertex_properties(...)
        return traversal

    def add_edge(self, elem):
        """Convenience function for generating crud traversals."""
        props = mapper.map_props_to_db(elem, elem.__mapping__)
        traversal = self.traversal().V(elem.source.id)
        traversal = traversal.addE(elem.__mapping__._label)
        traversal = traversal.to(
            self.traversal().V(elem.target.id))
        traversal, _ = self.add_properties(traversal, props)
        return traversal

    # def add_vertex_properties(self, traversal, props):
    #     # refactor
    #     binding = 0
    #     potential_removals = []
    #     for k, v in props:
    #         if v:
    #             if isinstance(v, element.VertexProperty):
    #                 v = v.value
    #             if isinstance(v, (list, set)):
    #                 new_val = []
    #                 for val in v:
    #                     if isinstance(val, element.VertexProperty):
    #                         val = val.value
    #                     new_val.append(val)
    #                 if isinstance(v, set):
    #                     cardinality = process.Cardinality.set
    #                 else:
    #                     cardinality = process.Cardinality.list
    #                 traversal = traversal.property(
    #                     cardinality,
    #                     ('k' + str(binding), k),
    #                     ('v' + str(binding), new_val))
    #             else:
    #                 traversal = traversal.property(
    #                     ('k' + str(binding), k),
    #                     ('v' + str(binding), v))
    #             binding += 1
    #         else:
    #             potential_removals.append(k)
    #     return traversal, potential_removals

    def add_properties(self, traversal, props):
        binding = 0
        potential_removals = []
        for card, db_name, val, metaprops in props:
            if val:
                key = ('k' + str(binding), db_name)
                val = ('v' + str(binding), val)
                if card:
                    if card == cardinality.Cardinality.list:
                        card = process.Cardinality.list
                    elif card == cardinality.Cardinality.set:
                        card = process.Cardinality.set
                    else:
                        card = process.Cardinality.single
                    traversal = traversal.property(card, key, val)
                else:
                    traversal = traversal.property(key, val)
                binding += 1
            else:
                potential_removals.append(db_name)
        return traversal, potential_removals
