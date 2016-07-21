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

import abc

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


class RemoteElement(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def __init__(self, id, label, properties=None, **attrs):
        self._id = id
        self._label = label
        self._properties = properties or dict()
        for k, v in attrs.items():
            setattr(self, k, v)

    def __str__(self):
        return "{type}[{id}]".format(type=self.__class__.__name__, id=self.id)

    @property
    def id(self):
        return self._id

    @property
    def label(self):
        return self._label

    def keys(self):
        return frozenset(self._properties.keys())

    @abc.abstractmethod
    def property(self, key):
        pass

    @abc.abstractmethod
    def value(self, key):
        pass

    @abc.abstractmethod
    def values(self, *keys):
        pass

    @abc.abstractmethod
    def properties(self, *keys):
        pass


class RemoteVertex(RemoteElement):

    _value_error_msg = ("Multiple properties exist on this vertex for key {0},"
                        " use RemoteVertex.{method}({0})")

    def __init__(self, id, label, properties=None, **attrs):
        properties = properties or dict()
        props = dict()
        for key, value_list in properties.items():
            props[key] = [item['value'] for item in value_list]
        super().__init__(id, label, props, **attrs)

    def property(self, key):
        props = self.properties(key)
        if not props:
            return None
        if len(props) > 1:
            raise ValueError(self._value_error_msg.format(key, method="properties"))
        return props[0]

    def value(self, key):
        props = self.properties(key)
        if not props:
            raise AttributeError('No property on this object with key: {}'.format(key))
        if len(props) > 1:
            raise ValueError(self._value_error_msg.format(key, method="values"))
        key, val = props[0]
        return val

    def values(self, *keys):
        return [v for k, v in self.properties(keys)]

    def properties(self, *keys):
        if keys:
            keys = (key for key in keys if key in self._properties)
        else:
            keys = self._properties.keys()

        props = list()
        for key in keys:
            for val in self._properties[key]:
                props.append((key, val))

        return props


class RemoteEdge(RemoteElement):
    def __init__(self, id, label, properties=None, **attrs):
        super().__init__(id, label, properties, **attrs)

    def property(self, key):
        prop = self.properties(key)
        if not prop:
            return None
        return prop[0]

    def value(self, key):
        return self._properties.get(key)

    def values(self, *keys):
        return [v for k, v in self.properties(keys)]

    def properties(self, *keys):
        if len(keys) > 0:
            props = ((key, self._properties[key]) for key in keys if key in self._properties)
        else:
            props = self._properties.items()
        return list(props)
