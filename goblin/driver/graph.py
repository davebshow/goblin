# Copyright 2016 David M. Brown
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

import functools

from gremlin_python.process.graph_traversal import (
    GraphTraversal, GraphTraversalSource)
from gremlin_python.process.traversal import TraversalStrategies
from gremlin_python.driver.remote_connection import (
    RemoteStrategy, RemoteTraversalSideEffects)
from gremlin_python.structure.graph import Graph
from goblin.driver.serializer import GraphSON2MessageSerializer


class AsyncRemoteTraversalSideEffects(RemoteTraversalSideEffects):

    async def keys(self):
        return await self.keys_lambda()

    async def get(self, key):
        return await self.value_lambda(sideEffectKey=key)


class AsyncRemoteStrategy(RemoteStrategy):

    async def apply(self, traversal):
        serializer = self.remote_connection.message_serializer
        if serializer is GraphSON2MessageSerializer:
            processor = 'traversal'
            op = 'bytecode'
            side_effects = AsyncRemoteTraversalSideEffects
        else:
            processor = ''
            op = 'eval'
            side_effects = None
        if traversal.traversers is None:
            resp = await self.remote_connection.submit(
                gremlin=traversal.bytecode, processor=processor, op=op)
            traversal.traversers = resp
            if side_effects:
                keys_lambda = functools.partial(self.remote_connection.submit,
                                                processor='traversal',
                                                op='keys',
                                                sideEffect=resp.request_id)
                value_lambda = functools.partial(self.remote_connection.submit,
                                                 processor='traversal',
                                                 op='gather',
                                                 sideEffect=resp.request_id)
                side_effects = side_effects(keys_lambda, value_lambda)
            traversal.side_effects = side_effects



class AsyncGraphTraversal(GraphTraversal):

    async def __aiter__(self):
        return self

    async def __anext__(self):
        if self.traversers is None:
            await self._get_traversers()
        if self.last_traverser is None:
            self.last_traverser = await self.traversers.fetch_data()
            if self.last_traverser is None:
                raise StopAsyncIteration
        obj = self.last_traverser.object
        self.last_traverser.bulk = self.last_traverser.bulk - 1
        if self.last_traverser.bulk <= 0:
            self.last_traverser = None
        return obj

    async def _get_traversers(self):
        for ts in self.traversal_strategies.traversal_strategies:
            await ts.apply(self)

    async def next(self, amount=None):
        """
        **coroutine** Return the next result from the iterator.

        :param int amount: The number of results returned, defaults to None
            (1 result)
        """
        if amount is None:
            try:
                return await self.__anext__()
            except StopAsyncIteration:
                pass
        else:
            count = 0
            tempList = []
            while count < amount:
                count = count + 1
                try: temp = await self.__anext__()
                except StopIteration: return tempList
                tempList.append(temp)
            return tempList

    async def toList(self):
        """**coroutine** Submit the travesal, iterate results, return a list"""
        results = []
        async for msg in self:
            results.append(msg)
        return results

    async def toSet(self):
        """**coroutine** Submit the travesal, iterate results, return a set"""
        results = set()
        async for msg in self:
            results.add(msg)
        return results

    async def oneOrNone(self):
        """
        **coroutine** Get one or zero results from a traveral. Returns last
        iterated result.
        """
        result = None
        async for msg in self:
            result = msg
        return result

    def iterate(self):
        raise NotImplementedError

    def nextTraverser(self):
        raise NotImplementedError


class AsyncGraph(Graph):
    """Generate asynchronous gremlin traversals using native Python"""

    def traversal(self, *, graph_traversal=None, remote_strategy=None):
        """
        Get a traversal source from the Graph

        :param gremlin_python.process.GraphTraversal graph_traversal:
            Custom graph traversal class
        :param gremlin_python.driver.remote_connection.RemoteStrategy remote_strategy:
            Custom remote strategy class

        :returns:
            :py:class:`gremlin_python.process.graph_traversal.GraphTraversalSource`
        """
        if graph_traversal is None:
            graph_traversal = AsyncGraphTraversal
        if remote_strategy is None:
            remote_strategy = AsyncRemoteStrategy
        return GraphTraversalSource(
            self, TraversalStrategies.global_cache[self.__class__],
            remote_strategy=remote_strategy,
            graph_traversal=graph_traversal)
