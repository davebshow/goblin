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
    GraphTraversalSource, GraphTraversal)
from gremlin_python.process.traversal import (
    TraversalStrategy, TraversalStrategies)


class AsyncGraphTraversal(GraphTraversal):
    def __init__(self, graph, traversal_strategies, bytecode):
        GraphTraversal.__init__(self, graph, traversal_strategies, bytecode)

    def __repr__(self):
        return self.graph.translator.translate(self.bytecode)

    def toList(self):
        raise NotImplementedError

    def toSet(self):
        raise NotImplementedError

    async def next(self):
        resp = await self.traversal_strategies.apply(self)
        return resp


class AsyncRemoteStrategy(TraversalStrategy):
    async def apply(self, traversal):
        result = await traversal.graph.remote_connection.submit(
            traversal.graph.translator.translate(traversal.bytecode),
            bindings=traversal.bindings,
            lang=traversal.graph.translator.target_language)
        return result


class AsyncGraph:
    def traversal(self):
        return GraphTraversalSource(self, self.traversal_strategy,
                                    graph_traversal=self.graph_traversal)


class AsyncRemoteGraph(AsyncGraph):
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
    def __init__(self, translator, remote_connection, *, graph_traversal=None):
        self.traversal_strategy = AsyncRemoteStrategy()  # A single traversal strategy
        self.translator = translator
        self.remote_connection = remote_connection
        if graph_traversal is None:
            graph_traversal = AsyncGraphTraversal
        self.graph_traversal = graph_traversal

    def __repr__(self):
        return "remotegraph[" + self.remote_connection.url + "]"

    async def close(self):
        """Close underlying remote connection"""
        await self.remote_connection.close()
        self.remote_connection = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()
