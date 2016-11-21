import asyncio
import uuid
from unittest import mock

import json
import pytest

import aiohttp
from aiohttp import client_ws

import goblin
from goblin import driver
from goblin.driver import serializer
from goblin import provider

request_id = uuid.UUID(int=215449331521667564889692237976543325869, version=4)


# based on this handy tip on SO: http://stackoverflow.com/a/29905620/6691423
def get_mock_coro(return_value):
    async def mock_coro(*args, **kwargs):
        return return_value

    return mock.Mock(wraps=mock_coro)


async def mock_receive():
    message = mock.Mock()
    message.tp = aiohttp.MsgType.close
    return message


async def mock_ws_connect(*args, **kwargs):
    mock_client = mock.Mock(spec=client_ws.ClientWebSocketResponse)
    mock_client.closed = False
    mock_client.receive = mock.Mock(wraps=mock_receive)
    mock_client.close = get_mock_coro(None)
    return mock_client


class TestProvider(provider.Provider):
    DEFAULT_OP_ARGS = {
        'standard': {
            'eval': {
                'fictional_argument': 'fictional_value'
            },
        },
        'session': {
            'eval': {
                'manageTransaction': True
            },

        }
    }

    @staticmethod
    def get_hashable_id(val):
        return val


def deserialize_json_request(request):
    header_len = request[0] + 1
    payload = request[header_len:]
    return json.loads(payload.decode())


@pytest.fixture(params=(
        serializer.GraphSONMessageSerializer,
        serializer.GraphSON2MessageSerializer
))
def message_serializer(request):
    return request.param


@pytest.mark.parametrize('processor_name,key,value', (
        ('standard', 'fictional_argument', 'fictional_value'),
        ('session', 'manageTransaction', True)
))
def test_get_processor_provider_default_args(processor_name, key, value):
    processor = serializer.GraphSONMessageSerializer.get_processor(TestProvider, processor_name)
    assert processor._default_args == TestProvider.DEFAULT_OP_ARGS[processor_name]
    eval_args = processor.get_op_args('eval', {'gremlin': 'g.V()'})
    assert eval_args['gremlin'] == 'g.V()'
    assert eval_args[key] == value


@pytest.mark.parametrize('processor,key,value', (
        ('', 'fictional_argument', 'fictional_value'),
        ('session', 'manageTransaction', True)
))
def test_serializer_default_op_args(message_serializer, processor, key, value):
    g = driver.AsyncGraph().traversal()
    traversal = g.V().hasLabel('stuff').has('foo', 'bar')
    serialized_message = message_serializer.serialize_message(
        TestProvider, str(uuid.uuid4()), processor=processor, op='eval', gremlin=traversal.bytecode)
    message = deserialize_json_request(serialized_message)
    assert message['args'][key] == value


@pytest.mark.parametrize('processor,key,value', (
        ('', 'fictional_argument', 'fictional_value'),
        ('session', 'manageTransaction', True)
))
@pytest.mark.asyncio
async def test_conn_default_op_args(event_loop, monkeypatch, processor, key, value):
    mock_client_session = mock.Mock(spec=aiohttp.ClientSession)
    mock_client_session_instance = mock.Mock(spec=aiohttp.ClientSession)
    mock_client_session.return_value = mock_client_session_instance
    mock_client_session_instance.ws_connect = mock.Mock(wraps=mock_ws_connect)
    mock_client_session_instance.close = get_mock_coro(None)  # otherwise awaiting ws.close is an error

    monkeypatch.setattr(aiohttp, 'ClientSession', mock_client_session)
    monkeypatch.setattr(uuid, 'uuid4', mock.Mock(return_value=request_id))

    conn = await driver.Connection.open(
        'some_url',
        event_loop,
        message_serializer=serializer.GraphSONMessageSerializer,
        provider=TestProvider
    )

    resp = await conn.submit(
        gremlin='g.V().hasLabel("foo").count()', processor=processor, op='eval')

    submitted_bytes = conn._ws.send_bytes.call_args[0][0]
    submitted_json = submitted_bytes[17:].decode()
    submitted_dict = json.loads(submitted_json)

    assert submitted_dict['args'][key] == value

    await conn.close()
    resp.close()


@pytest.mark.asyncio
async def test_cluster_conn_provider(event_loop, gremlin_host, gremlin_port):
    cluster = await driver.Cluster.open(
        event_loop, provider=TestProvider, hosts=[gremlin_host], port=gremlin_port)
    assert cluster.config['provider'] == TestProvider

    pooled_conn = await cluster.get_connection()
    assert pooled_conn._conn._provider == TestProvider

    await cluster.close()


@pytest.mark.asyncio
async def test_app_cluster_provider(event_loop):
    app = await goblin.Goblin.open(event_loop, provider=TestProvider)
    assert app._provider is TestProvider
    assert app._cluster.config['provider'] is TestProvider

    await app.close()


@pytest.mark.asyncio
async def test_app_provider_hashable_id(event_loop):
    app = await goblin.Goblin.open(event_loop, provider=TestProvider)
    assert app._get_hashable_id is TestProvider.get_hashable_id

    await app.close()
