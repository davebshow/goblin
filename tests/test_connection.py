import asyncio
import base64
import json

import aiohttp
import pytest
from aiogremlin import exception
from aiohttp import web
from gremlin_python.driver import request

from goblin import driver, provider


@pytest.mark.asyncio
async def test_get_close_conn(connection):
    ws = connection._transport
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
        message = request.RequestMessage(
            processor='', op='eval', args={
                'gremlin': '1 + 1'
            })
        stream = await connection.write(message)
        results = []
        async for msg in stream:
            results.append(msg)
        assert len(results) == 1
        assert results[0] == 2


@pytest.mark.asyncio
async def test_204_empty_stream(connection, aliases):
    resp = False
    async with connection:
        message = request.RequestMessage(
            processor='',
            op='eval',
            args={
                'gremlin': 'g.V().has("unlikely", "even less likely")'
            })
        stream = await connection.write(message)
        async for msg in stream:
            resp = True
    assert not resp


@pytest.mark.asyncio
async def test_server_error(connection):
    async with connection:
        message = request.RequestMessage(
            processor='', op='eval', args={
                'gremlin': 'g. V jla;sdf'
            })
        with pytest.raises(exception.GremlinServerError):
            stream = await connection.write(message)
            async for msg in stream:
                pass


@pytest.mark.asyncio
async def test_cant_connect(event_loop, gremlin_server, unused_server_url):
    with pytest.raises(Exception):
        await gremlin_server.get_connection(unused_server_url, event_loop)


@pytest.mark.asyncio
async def test_resp_queue_removed_from_conn(connection):
    async with connection:
        message = request.RequestMessage(
            processor='', op='eval', args={
                'gremlin': '1 + 1'
            })
        stream = await connection.write(message)
        async for msg in stream:
            pass
        await asyncio.sleep(0)
        assert stream._response_queue not in list(
            connection._result_sets.values())


@pytest.mark.asyncio
async def test_stream_done(connection):
    async with connection:
        message = request.RequestMessage(
            processor='', op='eval', args={
                'gremlin': '1 + 1'
            })
        stream = await connection.write(message)
        async for msg in stream:
            pass
        assert stream.done


@pytest.mark.asyncio
async def test_connection_response_timeout(connection):
    async with connection:
        message = request.RequestMessage(
            processor='', op='eval', args={
                'gremlin': '1 + 1'
            })
        connection._response_timeout = 0.0000001
        with pytest.raises(exception.ResponseTimeoutError):
            stream = await connection.write(message)
            async for msg in stream:
                pass


# @pytest.mark.asyncio
# async def test_authenticated_connection(event_loop, unused_tcp_port):
#     authentication_request_queue = asyncio.Queue(loop=event_loop)
#
#     username, password = 'test_username', 'test_password'
#
#     async def fake_auth(request):
#         ws = web.WebSocketResponse()
#         await ws.prepare(request)
#
#         msg = await ws.receive()
#         data = json.loads(msg.data.decode()[17:])
#         await authentication_request_queue.put(data)
#
#         auth_resp = {
#             "requestId": data["requestId"],
#             "status": {"code": 407, "attributes": {}, "message": ""},
#             "result": {"data": None, "meta": {}}
#         }
#         resp_payload = json.dumps(auth_resp)
#         ws.send_str(resp_payload)
#
#         auth_msg = await ws.receive()
#         auth_msg_data = json.loads(auth_msg.data.decode()[17:])
#         await authentication_request_queue.put(auth_msg_data)
#
#         return ws
#
#     aiohttp_app = web.Application(loop=event_loop)
#     aiohttp_app.router.add_route('GET', '/gremlin', fake_auth)
#     handler = aiohttp_app.make_handler()
#     srv = await event_loop.create_server(handler, '0.0.0.0', unused_tcp_port)
#
#     async with aiohttp.ClientSession(loop=event_loop) as session:
#         url = 'ws://0.0.0.0:{}/gremlin'.format(unused_tcp_port)
#         async with session.ws_connect(url) as ws_client:
#             connection = driver.Connection(
#                 url=url, ws=ws_client, loop=event_loop, client_session=session,
#                 username=username, password=password, max_inflight=64, response_timeout=None,
#                 message_serializer=driver.GraphSONMessageSerializer,
#                 provider=provider.TinkerGraph
#             )
#             message = request.RequestMessage(
#                 processor='', op='eval',
#                 args={'gremlin': '1 + 1'})
#             task = event_loop.create_task(connection.write(message))
#             initial_request = await authentication_request_queue.get()
#             auth_request = await authentication_request_queue.get()
#             print(auth_request)
#             auth_str = auth_request['args']['sasl']
#             assert base64.b64decode(auth_str).decode().split('\x00')[1:] == [username, password]
#             assert auth_request['requestId'] == initial_request['requestId']
#             resp = await task
#             resp.close()
#
#             await connection.close()
