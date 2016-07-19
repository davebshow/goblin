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

from goblin import exception


@pytest.mark.asyncio
async def test_get_close_conn(connection):
    ws = connection._ws
    assert not ws.closed
    assert not connection.closed
    await connection.close()
    assert connection.closed
    assert ws.closed


@pytest.mark.asyncio
async def test_conn_context_manager(connection):
    async with connection:
        assert not connection.closed
    assert connection.closed


@pytest.mark.asyncio
async def test_submit(connection):
    async with connection:
        stream = await connection.submit("1 + 1")
        results = []
        async for msg in stream:
            results.append(msg)
        assert len(results) == 1
        assert results[0] == 2


@pytest.mark.asyncio
async def test_204_empty_stream(connection):
    resp = False
    async with connection:
        stream = await connection.submit('g.V().has("unlikely", "even less likely")')
        async for msg in stream:
            resp = True
    assert not resp


@pytest.mark.asyncio
async def test_server_error(connection):
    async with connection:
        stream = await connection.submit('g. V jla;sdf')
        with pytest.raises(exception.GremlinServerError):
            async for msg in stream:
                pass


@pytest.mark.asyncio
async def test_cant_connect(event_loop, gremlin_server, unused_server_url):
    with pytest.raises(Exception):
        await gremlin_server.open(unused_server_url, event_loop)


@pytest.mark.asyncio
async def test_resp_queue_removed_from_conn(connection):
    async with connection:
        stream = await connection.submit("1 + 1")
        async for msg in stream:
            pass
        assert stream._response_queue not in list(
            connection._response_queues.values())


@pytest.mark.asyncio
async def test_stream_done(connection):
    async with connection:
        stream = await connection.submit("1 + 1")
        async for msg in stream:
            pass
        assert stream._done
