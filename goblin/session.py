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

from goblin import exception, mapper, traversal
from goblin.driver import connection, graph
from goblin.element import GenericVertex


logger = logging.getLogger(__name__)


class Session(connection.AbstractConnection):
    """
    Provides the main API for interacting with the database. Does not
    necessarily correpsond to a database session. Don't instantiate directly,
    instead use :py:meth:`Goblin.session<goblin.app.Goblin.session>`.

    :param goblin.app.Goblin app:
    :param goblin.driver.connection conn:
    :param bool use_session: Support for Gremlin Server session. Not implemented
    """

    def __init__(self, app, conn, get_hashable_id, *, use_session=False,
                 aliases=None):
        self._app = app
        self._conn = conn
        self._loop = self._app._loop
        self._use_session = False
        self._aliases = aliases or dict()
        self._pending = collections.deque()
        self._current = weakref.WeakValueDictionary()
        self._get_hashable_id = get_hashable_id
        remote_graph = graph.AsyncRemoteGraph(
            self._app.translator, self,
            graph_traversal=traversal.GoblinTraversal)
        self._traversal_factory = traversal.TraversalFactory(remote_graph)

    @property
    def app(self):
        return self._app

    @property
    def conn(self):
        return self._conn

    @property
    def traversal_factory(self):
        return self._traversal_factory

    @property
    def current(self):
        return self._current

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    async def close(self):
        """
        Close the underlying db connection and disconnect session from Goblin
        application.
        """
        await self.conn.close()
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
        return self.traversal_factory.traversal()

    def traversal(self, element_class):
        """
        Get a traversal spawned from an element class.

        :param :goblin.element.Element element_class: Element class
            used to spawn traversal.

        :returns: :py:class:`GoblinTraversal<goblin.traversal.GoblinTraversal>`
            object
        """
        return self.traversal_factory.traversal(element_class=element_class)

    async def submit(self,
                    gremlin,
                    *,
                    bindings=None,
                    lang='gremlin-groovy'):
        """
        Submit a query to the Gremiln Server.

        :param str gremlin: Gremlin script to submit to server.
        :param dict bindings: A mapping of bindings for Gremlin script.
        :param str lang: Language of scripts submitted to the server.
            "gremlin-groovy" by default

        :returns:
            :py:class:`TraversalResponse<goblin.traversal.TraversalResponse>`
            object
        """
        await self.flush()
        async_iter = await self.conn.submit(
            gremlin, bindings=bindings, lang=lang, aliases=self._aliases)
        response_queue = asyncio.Queue(loop=self._loop)
        self._loop.create_task(
            self._receive(async_iter, response_queue))
        return traversal.TraversalResponse(response_queue)

    async def _receive(self, async_iter, response_queue):
        async for result in async_iter:
            if (isinstance(result, dict) and
                    result.get('type', '') in ['vertex', 'edge']):
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
                response_queue.put_nowait(element)
            else:
                response_queue.put_nowait(result)
        response_queue.put_nowait(None)

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
        traversal = self.traversal_factory.remove_vertex(vertex)
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
        traversal = self.traversal_factory.remove_edge(edge)
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
        return await self.traversal_factory.get_vertex_by_id(
            vertex).one_or_none()

    async def get_edge(self, edge):
        """
        Get a edge from the db. Edge must have id.

        :param goblin.element.Edge element: Edge to be retrieved

        :returns: :py:class:`Edge<goblin.element.Edge>` | None
        """
        return await self.traversal_factory.get_edge_by_id(
            edge).one_or_none()

    async def update_vertex(self, vertex):
        """
        Update a vertex, generally to change/remove property values.

        :param goblin.element.Vertex vertex: Vertex to be updated

        :returns: :py:class:`Vertex<goblin.element.Vertex>` object
        """
        props = mapper.map_props_to_db(vertex, vertex.__mapping__)
        # vert_props = mapper.map_vert_props_to_db
        traversal = self.g.V(vertex.id)
        return await self._update_vertex_properties(vertex, traversal, props)

    async def update_edge(self, edge):
        """
        Update an edge, generally to change/remove property values.

        :param goblin.element.Edge edge: Edge to be updated

        :returns: :py:class:`Edge<goblin.element.Edge>` object
        """
        props = mapper.map_props_to_db(edge, edge.__mapping__)
        traversal = self.g.E(edge.id)
        return await self._update_edge_properties(edge, traversal, props)

    # Transaction support
    def tx(self):
        """Not implemented"""
        raise NotImplementedError

    def _wrap_in_tx(self):
        raise NotImplementedError

    async def commit(self):
        """Not implemented"""
        await self.flush()
        if self.engine._features['transactions'] and self._use_session():
            await self.tx()
        raise NotImplementedError

    async def rollback(self):
        raise NotImplementedError

    # *metodos especiales privados for creation API
    async def _simple_traversal(self, traversal, element):
        stream = await self.conn.submit(
            repr(traversal), bindings=traversal.bindings,
            aliases=self._aliases)
        msg = await stream.fetch_data()
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

    async def _add_vertex(self, elem):
        """Convenience function for generating crud traversals."""
        props = mapper.map_props_to_db(elem, elem.__mapping__)
        traversal = self.g.addV(elem.__mapping__.label)
        traversal, _, metaprops = self.traversal_factory.add_properties(
            traversal, props)
        result = await self._simple_traversal(traversal, elem)
        if metaprops:
            await self._add_metaprops(result, metaprops)
            traversal = self.traversal_factory.get_vertex_by_id(elem)
            result = await self._simple_traversal(traversal, elem)
        return result

    async def _add_edge(self, elem):
        """Convenience function for generating crud traversals."""
        props = mapper.map_props_to_db(elem, elem.__mapping__)
        traversal = self.g.V(elem.source.id)
        traversal = traversal.addE(elem.__mapping__._label)
        traversal = traversal.to(
            self.g.V(elem.target.id))
        traversal, _, _ = self.traversal_factory.add_properties(
            traversal, props)
        return await self._simple_traversal(traversal, elem)

    async def _check_vertex(self, vertex):
        """Used to check for existence, does not update session vertex"""
        traversal = self.g.V(vertex.id)
        stream = await self.conn.submit(repr(traversal), aliases=self._aliases)
        return await stream.fetch_data()

    async def _check_edge(self, edge):
        """Used to check for existence, does not update session edge"""
        traversal = self.g.E(edge.id)
        stream = await self.conn.submit(repr(traversal), aliases=self._aliases)
        return await stream.fetch_data()

    async def _update_vertex_properties(self, vertex, traversal, props):
        traversal, removals, metaprops = self.traversal_factory.add_properties(
            traversal, props)
        for k in removals:
            await self.g.V(vertex.id).properties(k).drop().one_or_none()
        result = await self._simple_traversal(traversal, vertex)
        if metaprops:
            removals = await self._add_metaprops(result, metaprops)
            for db_name, key, value in removals:
                await self.g.V(vertex.id).properties(
                    db_name).has(key, value).drop().one_or_none()
            traversal = self.traversal_factory.get_vertex_by_id(vertex)
            result = await self._simple_traversal(traversal, vertex)
        return result

    async def _update_edge_properties(self, edge, traversal, props):
        traversal, removals, _ = self.traversal_factory.add_properties(
            traversal, props)
        for k in removals:
            await self.g.E(edge.id).properties(k).drop().one_or_none()
        return await self._simple_traversal(traversal, edge)

    async def _add_metaprops(self, result, metaprops):
        potential_removals = []
        for metaprop in metaprops:
            db_name, (binding, value), metaprops = metaprop
            for key, val in metaprops.items():
                if val:
                    traversal = self.g.V(result.id).properties(
                        db_name).hasValue(value).property(key, val)
                    stream = await self.conn.submit(
                        repr(traversal), bindings=traversal.bindings,
                        aliases=self._aliases)
                    await stream.fetch_data()
                else:
                    potential_removals.append((db_name, key, value))
        return potential_removals
