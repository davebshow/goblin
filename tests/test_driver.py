import pytest


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
        with pytest.raises(Exception):
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
