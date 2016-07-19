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

from gremlin_python import process


@pytest.mark.asyncio
async def test_close_graph(remote_graph):
    remote_connection = remote_graph.remote_connection
    await remote_graph.close()
    assert remote_connection.closed


@pytest.mark.asyncio
async def test_conn_context_manager(remote_graph):
    remote_connection = remote_graph.remote_connection
    async with remote_graph:
        assert not remote_graph.remote_connection.closed
    assert remote_connection.closed


@pytest.mark.asyncio
async def test_generate_traversal(remote_graph):
    async with remote_graph:
        traversal = remote_graph.traversal().V().hasLabel(('v1', 'person'))
        assert isinstance(traversal, process.GraphTraversal)
        assert traversal.bindings['v1'] == 'person'


@pytest.mark.asyncio
async def test_submit_traversal(remote_graph):
    async with remote_graph:
        g = remote_graph.traversal()
        resp = await g.addV('person').property('name', 'leifur').next()
        leif = await resp.fetch_data()
        assert leif['properties']['name'][0]['value'] == 'leifur'
        assert leif['label'] == 'person'
        resp = await g.V(leif['id']).drop().next()
        none = await resp.fetch_data()
        assert none is None
