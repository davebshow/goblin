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

from goblin import exception
from goblin.driver import serializer


@pytest.mark.asyncio
async def test_eval(remote_graph, connection, aliases):
    async with connection:
        connection._message_serializer = serializer.GraphSON2MessageSerializer()
        g = remote_graph.traversal()
        traversal = "g.addV('person').property('name', 'leifur')"
        resp = await connection.submit(
            processor='', op='eval', gremlin=traversal, scriptEvalTimeout=1, aliases=aliases)

        async for msg in resp:
            assert msg['label'] == 'person'


@pytest.mark.asyncio
async def test_bytecode(remote_graph, connection, aliases):
    async with connection:
        connection._message_serializer = serializer.GraphSON2MessageSerializer()
        g = remote_graph.traversal()
        traversal = g.addV('person').property('name', 'leifur')
        resp = await connection.submit(
            processor='traversal', op='bytecode', gremlin=traversal.bytecode, aliases=aliases)
        async for msg in resp:
            vid = msg.id
        traversal = g.V(vid).label()
        resp = await connection.submit(
            processor='traversal', op='bytecode', gremlin=traversal.bytecode, aliases=aliases)
        async for msg in resp:
            assert msg == 'person'
        traversal = g.V(vid).name
        resp = await connection.submit(
            processor='traversal', op='bytecode', gremlin=traversal.bytecode, aliases=aliases)
        async for msg in resp:
            assert msg == 'leifur'


@pytest.mark.asyncio
async def test_side_effects(remote_graph, connection, aliases):
    async with connection:
        connection._message_serializer = serializer.GraphSON2MessageSerializer()
        g = remote_graph.traversal()
        # Add some nodes
        traversal = g.addV('person').property('name', 'leifur')
        resp = await connection.submit(
            processor='traversal', op='bytecode', gremlin=traversal.bytecode, aliases=aliases)
        async for msg in resp:
            pass
        traversal = g.addV('person').property('name', 'dave')
        resp = await connection.submit(
            processor='traversal', op='bytecode', gremlin=traversal.bytecode, aliases=aliases)
        async for msg in resp:
            pass
        traversal = g.addV('person').property('name', 'jonathan')
        resp = await connection.submit(
            processor='traversal', op='bytecode', gremlin=traversal.bytecode, aliases=aliases)
        async for msg in resp:
            pass

        # # Make a query
        traversal = g.V().aggregate('a').aggregate('b')
        resp = await connection.submit(
            processor='traversal', op='bytecode', gremlin=traversal.bytecode, aliases=aliases)
        request_id = resp.request_id
        async for msg in resp:
            pass
        resp = await connection.submit(processor='traversal', op='keys',
                                       sideEffect=request_id, aliases=aliases)
        keys = []
        async for msg in resp:
            keys.append(msg)
        assert keys == ['a', 'b']

        resp = await connection.submit(processor='traversal', op='gather',
                                       sideEffect=request_id,
                                       sideEffectKey='a', aliases=aliases)
        side_effects = []
        async for msg in resp:
            side_effects.append(msg)
        assert side_effects

        # Close isn't implmented yet
        # resp = await connection.submit(processor='traversal', op='close',
        #                                sideEffect=request_id)
        # async for msg in resp:
        #     print(msg)


@pytest.mark.asyncio
async def test_session(connection, aliases):
    async with connection:
        connection._message_serializer = serializer.GraphSON2MessageSerializer()
        session = str(uuid.uuid4())
        resp = await connection.submit(
            gremlin="v = g.addV('person').property('name', 'unused_name').next(); v",
            processor='session',
            op='eval',
            session=session,
            aliases=aliases)
        async for msg in resp:
            assert msg['label'] == 'person'
        resp = await connection.submit(
            gremlin="v.values('name')",
            processor='session',
            op='eval',
            session=session,
            aliases=aliases)
        async for msg in resp:
            assert msg == 'unused_name'
        # Close isnt' implemented yet
        # resp = await connection.submit(
        #     processor='session',
        #     op='close',
        #     session=session)
        # async for msg in resp:
        #     print(msg)
