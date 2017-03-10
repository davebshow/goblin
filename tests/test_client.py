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

import asyncio
import uuid

import pytest

from goblin.driver import GremlinServer


@pytest.mark.asyncio
async def test_client_auto_release(cluster):
    client = await cluster.connect()
    resp = await client.submit("1 + 1")
    async for msg in resp:
        pass
    await asyncio.sleep(0)
    host = cluster._hosts.popleft()
    assert len(host._pool._available) == 1
    await host.close()


@pytest.mark.asyncio
async def test_alias(cluster):
    client = await cluster.connect()
    aliased_client = client.alias({"g": "g1"})
    assert aliased_client._aliases == {"g": "g1"}
    assert aliased_client._cluster is client._cluster
    assert aliased_client._loop is client._loop
    await cluster.close()


# @pytest.mark.asyncio
# async def test_sessioned_client(cluster):
#     session = str(uuid.uuid4())
#     client = await cluster.connect(session=session)
#     assert isinstance(client.cluster, GremlinServer)
#     resp = await client.submit("v = g.addV('person').property('name', 'joe').next(); v")
#     async for msg in resp:
#         try:
#             assert msg['properties']['name'][0]['value'] == 'joe'
#         except KeyError:
#             assert msg['properties']['name'][0]['@value']['value'] == 'joe'
#
#     resp = await client.submit("g.V(v.id()).values('name')")
#
#     async for msg in resp:
#         assert msg == 'joe'
#     await cluster.close()
