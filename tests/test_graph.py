import pytest
from goblin.gremlin_python import process


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
