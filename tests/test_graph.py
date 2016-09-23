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

import pytest

from goblin.driver import serializer

from gremlin_python import process


@pytest.mark.asyncio
async def test_generate_traversal(remote_graph, connection):
    async with connection:
        g = remote_graph.traversal().withRemote(connection)
        traversal = g.V().hasLabel(('v1', 'person'))
        assert isinstance(traversal, process.graph_traversal.GraphTraversal)
        assert traversal.bytecode.bindings['v1'] == 'person'


@pytest.mark.asyncio
async def test_submit_traversal(remote_graph, connection):
    async with connection:
        g = remote_graph.traversal().withRemote(connection)
        resp = g.addV('person').property('name', 'leifur')
        leif = await resp.next()
        resp.traversers.close()
        assert leif['properties']['name'][0]['value'] == 'leifur'
        assert leif['label'] == 'person'
        resp = g.V(leif['id']).drop()
        none = await resp.next()
        assert none is None


@pytest.mark.asyncio
async def test_side_effects(remote_graph, connection):
    async with connection:
        connection._message_serializer = serializer.GraphSON2MessageSerializer()
        g = remote_graph.traversal().withRemote(connection)
        # create some nodes
        resp = g.addV('person').property('name', 'leifur')
        leif = await resp.next()
        resp.traversers.close()
        resp = g.addV('person').property('name', 'dave')
        dave = await resp.next()
        resp.traversers.close()
        resp = g.addV('person').property('name', 'jon')
        jonthan = await resp.next()
        resp.traversers.close()
        traversal = g.V().aggregate('a').aggregate('b')
        async for msg in traversal:
            pass
        keys = []
        resp = await traversal.side_effects.keys()
        async for msg in resp:
            keys.append(msg)
        assert keys == ['a', 'b']
        side_effects = []
        resp = await traversal.side_effects.get('a')
        async for msg in resp:
            side_effects.append(msg)
        assert side_effects
        side_effects = []
        resp = await traversal.side_effects.get('b')
        async for msg in resp:
            side_effects.append(msg)
        assert side_effects
