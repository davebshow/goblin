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
import json

import base64
import pytest

import aiohttp
from aiohttp import web

from goblin import driver
from goblin import exception
from goblin import provider


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
        stream = await connection.submit(gremlin="1 + 1")
        results = []
        async for msg in stream:
            results.append(msg)
        assert len(results) == 1
        assert results[0] == 2


@pytest.mark.asyncio
async def test_204_empty_stream(connection, aliases):
    resp = False
    async with connection:
        stream = await connection.submit(
            gremlin='g.V().has("unlikely", "even less likely")',
            aliases=aliases
        )
        async for msg in stream:
            resp = True
    assert not resp


@pytest.mark.asyncio
async def test_server_error(connection):
    async with connection:
        stream = await connection.submit(gremlin='g. V jla;sdf')
        with pytest.raises(exception.GremlinServerError):
            async for msg in stream:
                pass


@pytest.mark.asyncio
async def test_cant_connect(event_loop, gremlin_server, unused_server_url):
    with pytest.raises(Exception):
        await gremlin_server.get_connection(unused_server_url, event_loop)


@pytest.mark.asyncio
async def test_resp_queue_removed_from_conn(connection):
    async with connection:
        stream = await connection.submit(gremlin="1 + 1")
        async for msg in stream:
            pass
        await asyncio.sleep(0)
        assert stream._response_queue not in list(
            connection._response_queues.values())


@pytest.mark.asyncio
async def test_stream_done(connection):
    async with connection:
        stream = await connection.submit(gremlin="1 + 1")
        async for msg in stream:
            pass
        assert stream.done

@pytest.mark.asyncio
async def test_connection_response_timeout(connection):
    async with connection:
        connection._response_timeout = 0.0000001
        with pytest.raises(exception.ResponseTimeoutError):
            stream = await connection.submit(gremlin="1 + 1")
            async for msg in stream:
                pass


@pytest.mark.asyncio
async def test_authenticated_connection(event_loop, unused_tcp_port):
    authentication_request_queue = asyncio.Queue(loop=event_loop)

    username, password = 'test_username', 'test_password'

    async def fake_auth(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)

        msg = await ws.receive()
        data = json.loads(msg.data.decode()[17:])
        await authentication_request_queue.put(data)

        auth_resp = {
            "requestId": data["requestId"],
            "status": {"code": 407, "attributes": {}, "message": ""},
            "result": {"data": None, "meta": {}}
        }
        resp_payload = json.dumps(auth_resp)
        ws.send_str(resp_payload)

        auth_msg = await ws.receive()
        auth_msg_data = json.loads(auth_msg.data.decode()[17:])
        await authentication_request_queue.put(auth_msg_data)

        return ws

    aiohttp_app = web.Application(loop=event_loop)
    aiohttp_app.router.add_route('GET', '/gremlin', fake_auth)
    handler = aiohttp_app.make_handler()
    srv = await event_loop.create_server(handler, '0.0.0.0', unused_tcp_port)

    async with aiohttp.ClientSession(loop=event_loop) as session:
        url = 'ws://0.0.0.0:{}/gremlin'.format(unused_tcp_port)
        async with session.ws_connect(url) as ws_client:
            connection = driver.Connection(
                url=url, ws=ws_client, loop=event_loop, client_session=session,
                username=username, password=password, max_inflight=64, response_timeout=None,
                message_serializer=driver.GraphSONMessageSerializer,
                provider=provider.TinkerGraph
            )
            task = event_loop.create_task(connection.submit(gremlin="1+1"))
            initial_request = await authentication_request_queue.get()
            auth_request = await authentication_request_queue.get()
            print(auth_request)
            auth_str = auth_request['args']['sasl']
            assert base64.b64decode(auth_str).decode().split('\x00')[1:] == [username, password]
            assert auth_request['requestId'] == initial_request['requestId']
            resp = await task
            resp.close()

            await connection.close()