import asyncio
import unittest

from goblin import driver


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
