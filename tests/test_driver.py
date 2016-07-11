import asyncio
import unittest

from goblin import driver
from goblin.driver import graph
from goblin.gremlin_python import process


class TestDriver(unittest.TestCase):

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)

    def test_open(self):

        async def go():
            connection = await driver.GremlinServer.open(
                "http://localhost:8182/", self.loop)
            async with connection:
                self.assertFalse(connection._ws.closed)
            self.assertTrue(connection._ws.closed)

        self.loop.run_until_complete(go())

    def test_open_as_ctx_mng(self):

        async def go():
            async with await driver.GremlinServer.open(
                    "http://localhost:8182/", self.loop) as connection:
                self.assertFalse(connection._ws.closed)
            self.assertTrue(connection._ws.closed)

        self.loop.run_until_complete(go())

    def test_submit(self):

        async def go():
            connection = await driver.GremlinServer.open(
                "http://localhost:8182/", self.loop)
            stream = await connection.submit("1 + 1")
            async for msg in stream:
                self.assertEqual(msg.data[0], 2)
            await connection.close()

        self.loop.run_until_complete(go())

    def test_async_graph(self):

        async def go():
            translator = process.GroovyTranslator('g')
            connection = await driver.GremlinServer.open(
                "http://localhost:8182/", self.loop)
            g = graph.AsyncRemoteGraph(translator, connection)
            traversal = g.traversal()
            resp = await traversal.V().next()
            async for msg in resp:
                print(msg)
            await connection.close()
        self.loop.run_until_complete(go())
