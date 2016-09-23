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

"""Main OGM API classes and constructors"""

import asyncio
import collections
import logging
import weakref

from goblin import cardinality, exception, mapper
from goblin.driver import connection, graph
from goblin.element import GenericVertex

from gremlin_python.driver.remote_connection import RemoteStrategy
from gremlin_python.process.traversal import Cardinality, Traverser



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
    def __init__(self, response_queue, request_id):
        self._queue = response_queue
        self._request_id = request_id
        self._done = False

    @property
    def request_id(self):
        return self._request_id

    async def __aiter__(self):
        return self

    async def __anext__(self):
        if self._done:
            return
        msg = await self.fetch_data()
        if msg:
            return msg
        else:
            self._done = True
            raise StopAsyncIteration

    async def fetch_data(self):
        return await self._queue.get()


class GoblinAsyncRemoteStrategy(RemoteStrategy):

    async def apply(self, traversal):

        if traversal.traversers is None:
            resp = await self.remote_connection.submit(
                gremlin=traversal.bytecode, processor='', op='eval')
            traversal.traversers = resp
            traversal.side_effects = None


class Session(connection.AbstractConnection):
    """
    Provides the main API for interacting with the database. Does not
    necessarily correpsond to a database session. Don't instantiate directly,
    instead use :py:meth:`Goblin.session<goblin.app.Goblin.session>`.

    :param goblin.app.Goblin app:
    :param goblin.driver.connection conn:
    :param bool use_session: Support for Gremlin Server session. Not implemented
    """

    def __init__(self, app, conn, get_hashable_id, transactions, *,
                 use_session=False):
        self._app = app
        self._conn = conn
        self._loop = self._app._loop
        self._use_session = False
        self._pending = collections.deque()
        self._current = weakref.WeakValueDictionary()
        self._get_hashable_id = get_hashable_id
        self._graph = graph.AsyncGraph()

    @property
    def graph(self):
        return self._graph

    @property
    def message_serializer(self):
        return self.conn.message_serializer

    @property
    def app(self):
        return self._app

    @property
    def conn(self):
        return self._conn

    @property
    def current(self):
        return self._current

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.close()

    def close(self):
        """
        """
        self._conn = None
        self._app = None

    # Traversal API
    @property
    def g(self):
        """
        Get a simple traversal source.

        :returns:
            :py:class:`goblin.gremlin_python.process.GraphTraversalSource`
            object
        """
        return self.traversal()

    @property
    def _g(self):
        """
        Traversal source for internal use. Uses undelying conn. Doesn't
        trigger complex deserailization.
        """
        return self.graph.traversal(
            graph_traversal=graph.AsyncGraphTraversal,
            remote_strategy=GoblinAsyncRemoteStrategy).withRemote(self.conn)

    def traversal(self, element_class=None):
        """
        Generate a traversal using a user defined element class as a
        starting point.

        :param goblin.element.Element element_class: An optional element
            class that will dictate the element type (vertex/edge) as well as
            the label for the traversal source

        :returns: :py:class:`AsyncGraphTraversal`
        """
        traversal = self.graph.traversal(
            graph_traversal=graph.AsyncGraphTraversal,
            remote_strategy=GoblinAsyncRemoteStrategy).withRemote(self)
        if element_class:
            label = element_class.__mapping__.label
            if element_class.__type__ == 'vertex':
                traversal = traversal.V()
            if element_class.__type__ == 'edge':
                traversal = traversal.E()
            traversal = traversal.hasLabel(label)
        return traversal

    async def submit(self,
                     **args):
        """
        Submit a query to the Gremiln Server.

        :param str gremlin: Gremlin script to submit to server.
        :param dict bindings: A mapping of bindings for Gremlin script.

        :returns:
            :py:class:`TraversalResponse<goblin.traversal.TraversalResponse>`
            object
        """
        await self.flush()
        async_iter = await self.conn.submit(**args)
        response_queue = asyncio.Queue(loop=self._loop)
        self._loop.create_task(
            self._receive(async_iter, response_queue))
        return TraversalResponse(response_queue, async_iter.request_id)

    async def _receive(self, async_iter, response_queue):
        async for result in async_iter:
            traverser = Traverser(self._deserialize_result(result), 1)
            response_queue.put_nowait(traverser)
        response_queue.put_nowait(None)

    def _deserialize_result(self, result):
        if isinstance(result, dict):
            if result.get('type', '') in ['vertex', 'edge']:
                hashable_id = self._get_hashable_id(result['id'])
                current = self.current.get(hashable_id, None)
                if not current:
                    element_type = result['type']
                    label = result['label']
                    if element_type == 'vertex':
                        current = self.app.vertices[label]()
                    else:
                        current = self.app.edges[label]()
                        current.source = GenericVertex()
                        current.target = GenericVertex()
                element = current.__mapping__.mapper_func(result, current)
                return element
            else:
                for key in result:
                    result[key] = self._deserialize_result(result[key])
                return result
        elif isinstance(result, list):
            return [self._deserialize_result(item) for item in result]
        else:
            return result

    # Creation API
    def add(self, *elements):
        """
        Add elements to session pending queue.

        :param goblin.element.Element elements: Elements to be added
        """
        for elem in elements:
            self._pending.append(elem)

    async def flush(self):
        """
        Issue creation/update queries to database for all elements in the
        session pending queue.
        """
        while self._pending:
            elem = self._pending.popleft()
            await self.save(elem)

    async def remove_vertex(self, vertex):
        """
        Remove a vertex from the db.

        :param goblin.element.Vertex vertex: Vertex to be removed
        """
        traversal = self._g.V(vertex.id).drop()
        result = await self._simple_traversal(traversal, vertex)
        hashable_id = self._get_hashable_id(vertex.id)
        vertex = self.current.pop(hashable_id)
        del vertex
        return result

    async def remove_edge(self, edge):
        """
        Remove an edge from the db.

        :param goblin.element.Edge edge: Element to be removed
        """
        traversal = self._g.E(edge.id).drop()
        result = await self._simple_traversal(traversal, edge)
        hashable_id = self._get_hashable_id(edge.id)
        edge = self.current.pop(hashable_id)
        del edge
        return result

    async def save(self, elem):
        """
        Save an element to the db.

        :param goblin.element.Element element: Vertex or Edge to be saved

        :returns: :py:class:`Element<goblin.element.Element>` object
        """
        if elem.__type__ == 'vertex':
            result = await self.save_vertex(elem)
        elif elem.__type__ == 'edge':
            result = await self.save_edge(elem)
        else:
            raise exception.ElementError(
                "Unknown element type: {}".format(elem.__type__))
        return result

    async def save_vertex(self, vertex):
        """
        Save a vertex to the db.

        :param goblin.element.Vertex element: Vertex to be saved

        :returns: :py:class:`Vertex<goblin.element.Vertex>` object
        """
        result = await self._save_element(
            vertex, self._check_vertex,
            self._add_vertex,
            self.update_vertex)
        hashable_id = self._get_hashable_id(result.id)
        self.current[hashable_id] = result
        return result

    async def save_edge(self, edge):
        """
        Save an edge to the db.

        :param goblin.element.Edge element: Edge to be saved

        :returns: :py:class:`Edge<goblin.element.Edge>` object
        """
        if not (hasattr(edge, 'source') and hasattr(edge, 'target')):
            raise exception.ElementError(
                "Edges require both source/target vertices")
        result = await self._save_element(
            edge, self._check_edge,
            self._add_edge,
            self.update_edge)
        hashable_id = self._get_hashable_id(result.id)
        self.current[hashable_id] = result
        return result

    async def get_vertex(self, vertex):
        """
        Get a vertex from the db. Vertex must have id.

        :param goblin.element.Vertex element: Vertex to be retrieved

        :returns: :py:class:`Vertex<goblin.element.Vertex>` | None
        """
        return await self.g.V(vertex.id).oneOrNone()

    async def get_edge(self, edge):
        """
        Get a edge from the db. Edge must have id.

        :param goblin.element.Edge element: Edge to be retrieved

        :returns: :py:class:`Edge<goblin.element.Edge>` | None
        """
        return await self.g.E(edge.id).oneOrNone()

    async def update_vertex(self, vertex):
        """
        Update a vertex, generally to change/remove property values.

        :param goblin.element.Vertex vertex: Vertex to be updated

        :returns: :py:class:`Vertex<goblin.element.Vertex>` object
        """
        props = mapper.map_props_to_db(vertex, vertex.__mapping__)
        # vert_props = mapper.map_vert_props_to_db
        traversal = self._g.V(vertex.id)
        return await self._update_vertex_properties(vertex, traversal, props)

    async def update_edge(self, edge):
        """
        Update an edge, generally to change/remove property values.

        :param goblin.element.Edge edge: Edge to be updated

        :returns: :py:class:`Edge<goblin.element.Edge>` object
        """
        props = mapper.map_props_to_db(edge, edge.__mapping__)
        traversal = self._g.E(edge.id)
        return await self._update_edge_properties(edge, traversal, props)

    # Transaction support
    def tx(self):
        """Not implemented"""
        raise NotImplementedError

    async def commit(self):
        """Not implemented"""
        await self.flush()
        if self.transactions and self._use_session():
            await self.tx()
        raise NotImplementedError

    async def rollback(self):
        raise NotImplementedError

    # *metodos especiales privados for creation API
    async def _simple_traversal(self, traversal, element):
        msg = await traversal.oneOrNone()
        if msg:
            msg = element.__mapping__.mapper_func(msg, element)
        return msg

    async def _save_element(self,
                            elem,
                            check_func,
                            create_func,
                            update_func):
        if hasattr(elem, 'id'):
            exists = await check_func(elem)
            if not exists:
                result = await create_func(elem)
            else:
                result = await update_func(elem)
        else:
            result = await create_func(elem)
        return result

    async def _add_vertex(self, vertex):
        """Convenience function for generating crud traversals."""
        props = mapper.map_props_to_db(vertex, vertex.__mapping__)
        traversal = self._g.addV(vertex.__mapping__.label)
        traversal, _, metaprops = self._add_properties(traversal, props)
        result = await self._simple_traversal(traversal, vertex)
        if metaprops:
            await self._add_metaprops(result, metaprops)
            traversal = self._g.V(vertex.id)
            result = await self._simple_traversal(traversal, vertex)
        return result

    async def _add_edge(self, edge):
        """Convenience function for generating crud traversals."""
        props = mapper.map_props_to_db(edge, edge.__mapping__)
        traversal = self._g.V(edge.source.id)
        traversal = traversal.addE(edge.__mapping__._label)
        traversal = traversal.to(
            self._g.V(edge.target.id))
        traversal, _, _ = self._add_properties(
            traversal, props)
        return await self._simple_traversal(traversal, edge)

    async def _check_vertex(self, vertex):
        """Used to check for existence, does not update session vertex"""
        msg = await self._g.V(vertex.id).oneOrNone()
        return msg

    async def _check_edge(self, edge):
        """Used to check for existence, does not update session edge"""
        msg = await self._g.E(edge.id).oneOrNone()
        return msg

    async def _update_vertex_properties(self, vertex, traversal, props):
        traversal, removals, metaprops = self._add_properties(traversal, props)
        for k in removals:
            await self._g.V(vertex.id).properties(k).drop().oneOrNone()
        result = await self._simple_traversal(traversal, vertex)
        if metaprops:
            removals = await self._add_metaprops(result, metaprops)
            for db_name, key, value in removals:
                await self._g.V(vertex.id).properties(
                    db_name).has(key, value).drop().oneOrNone()
            traversal = self._g.V(vertex.id)
            result = await self._simple_traversal(traversal, vertex)
        return result

    async def _update_edge_properties(self, edge, traversal, props):
        traversal, removals, _ = self._add_properties(traversal, props)
        for k in removals:
            await self._g.E(edge.id).properties(k).drop().oneOrNone()
        return await self._simple_traversal(traversal, edge)

    async def _add_metaprops(self, result, metaprops):
        potential_removals = []
        for metaprop in metaprops:
            db_name, (binding, value), metaprops = metaprop
            for key, val in metaprops.items():
                if val:
                    traversal = self._g.V(result.id).properties(
                        db_name).hasValue(value).property(key, val)
                    await traversal.oneOrNone()
                else:
                    potential_removals.append((db_name, key, value))
        return potential_removals

    def _add_properties(self, traversal, props):
        binding = 0
        potential_removals = []
        potential_metaprops = []
        for card, db_name, val, metaprops in props:
            if val:
                key = ('k' + str(binding), db_name)
                val = ('v' + str(binding), val)
                if card:
                    # Maybe use a dict here as a translator
                    if card == cardinality.Cardinality.list:
                        card = Cardinality.list
                    elif card == cardinality.Cardinality.set:
                        card = Cardinality.set
                    else:
                        card = Cardinality.single
                    traversal = traversal.property(card, key, val)
                else:
                    traversal = traversal.property(key, val)
                binding += 1
                if metaprops:
                    potential_metaprops.append((db_name, val, metaprops))
            else:
                potential_removals.append(db_name)
        return traversal, potential_removals, potential_metaprops
