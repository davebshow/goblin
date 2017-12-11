import pytest
from aiogremlin import process
from gremlin_python.process.traversal import Binding

from goblin import driver


@pytest.mark.asyncio
async def test_generate_traversal(remote_graph, remote_connection):
    async with remote_connection:
        g = remote_graph.traversal().withRemote(remote_connection)
        traversal = g.V().hasLabel(('v1', 'person'))
        assert isinstance(traversal,
                          process.graph_traversal.AsyncGraphTraversal)
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


@pytest.mark.skipif(
    pytest.config.getoption('provider') == 'dse', reason="need custom alias")
@pytest.mark.asyncio
async def test_side_effects(remote_graph, remote_connection):
    async with remote_connection:
        remote_connection._message_serializer = \
            driver.GraphSONMessageSerializer
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
