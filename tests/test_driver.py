import asyncio
import unittest

from goblin.gremlin_python_driver.driver import create_connection


class TestDriver(unittest.TestCase):

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)

    def test_connect(self):

        async def go():
            async with create_connection("http://localhost:8182/", self.loop) as conn:
                self.assertFalse(conn._ws.closed)

        self.loop.run_until_complete(go())

    def test_submit(self):

        async def go():
            async with create_connection("http://localhost:8182/", self.loop) as conn:
                async for msg in conn.submit("1 + 1"):
                    self.assertEqual(msg.data[0], 2)

        self.loop.run_until_complete(go())
