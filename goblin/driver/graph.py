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

"""A temporary solution to allow integration with gremlin_python package."""

from gremlin_python.process.graph_traversal import (
    GraphTraversal, GraphTraversalSource)
from gremlin_python.process.traversal import TraversalStrategies
from gremlin_python.driver.remote_connection import (
    RemoteStrategy, RemoteTraversalSideEffects)
from gremlin_python.structure.graph import Graph
from goblin.driver.serializer import GraphSON2MessageSerializer


class AsyncRemoteTraversalSideEffects(RemoteTraversalSideEffects):
    pass



class AsyncRemoteStrategy(RemoteStrategy):

    async def apply(self, traversal):
        if isinstance(self.remote_connection.message_serializer,
                      GraphSON2MessageSerializer):
            processor = 'traversal'
            op = 'bytecode'
            side_effects = RemoteTraversal
        else:
            processor = ''
            op = 'eval'
            side_effects = None
        if traversal.traversers is None:
            remote_traversal = await self.remote_connection.submit(
                gremlin=traversal.bytecode, processor=processor, op=op)
            traversal.side_effects = side_effects
            traversal.traversers = remote_traversal#.traversers


class AsyncGraphTraversal(GraphTraversal):

    # def __init__(self, graph, traversal_strategies, bytecode):
    #     GraphTraversal.__init__(self, graph, traversal_strategies, bytecode)

    def toList(self):
        raise NotImplementedError

    def toSet(self):
        raise NotImplementedError

    async def next(self):
        for ts in self.traversal_strategies.traversal_strategies:
            await ts.apply(self)
        return self.traversers


class AsyncGraph(Graph):
    """
    Generate asynchronous gremlin traversals using native Python.

    :param gremlin_python.process.GroovyTranslator translator:
        gremlin_python translator class, typically
        :py:class:`GroovyTranslator<gremlin_python.process.GroovyTranslator>`
    :param goblin.driver.connection connection: underlying remote
        connection
    :param gremlin_python.process.GraphTraversal graph_traversal:
        Custom graph traversal class
    """

    def traversal(self, *, graph_traversal=None):
        if graph_traversal is None:
            graph_traversal = AsyncGraphTraversal
        return GraphTraversalSource(
            self, TraversalStrategies.global_cache[self.__class__],
            remote_strategy=AsyncRemoteStrategy,
            graph_traversal=graph_traversal)
