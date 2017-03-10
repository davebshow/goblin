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

import pytest

from goblin import driver

from aiogremlin.gremlin_python import process
from aiogremlin.gremlin_python.process.traversal import Binding


@pytest.mark.asyncio
async def test_generate_traversal(remote_graph, remote_connection):
    async with remote_connection:
        g = remote_graph.traversal().withRemote(remote_connection)
        traversal = g.V().hasLabel(('v1', 'person'))
        assert isinstance(traversal, process.graph_traversal.GraphTraversal)
        assert traversal.bytecode.bindings['v1'] == 'person'


@pytest.mark.asyncio
async def test_submit_traversal(remote_graph, remote_connection):
    g = remote_graph.traversal().withRemote(remote_connection)
    resp = g.addV('person').property('name', 'leifur').valueMap(True)
    leif = await resp.next()
    assert leif['name'][0] == 'leifur'
    assert leif['label'] == 'person'
    resp = g.V(Binding('vid', leif['id'])).drop()
    none = await resp.next()
    assert none is None

    await remote_connection.close()


@pytest.mark.skipif(pytest.config.getoption('provider') == 'dse', reason="need custom alias")
@pytest.mark.asyncio
async def test_side_effects(remote_graph, remote_connection):
    async with remote_connection:
        remote_connection._message_serializer = driver.GraphSONMessageSerializer
        g = remote_graph.traversal().withRemote(remote_connection)
        # create some nodes
        resp = g.addV('person').property('name', 'leifur')
        leif = await resp.next()
        resp = g.addV('person').property('name', 'dave')
        dave = await resp.next()

        resp = g.addV('person').property('name', 'jon')
        jonthan = await resp.next()

        traversal = g.V().aggregate('a').aggregate('b')
        await traversal.iterate()
        keys = await traversal.side_effects.keys()
        assert keys == set(['a', 'b'])
        side_effects = await traversal.side_effects.get('a')
        assert side_effects
        side_effects = await traversal.side_effects.get('b')
        assert side_effects
