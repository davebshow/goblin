import asyncio
import unittest

from goblin.gremlin_python_driver.driver import Driver


class TestDriver(unittest.TestCase):

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)

    def test_connect(self):

        async def go():
            driver = Driver("http://localhost:8182/", self.loop)
            async with driver.get() as conn:
                self.assertFalse(conn._ws.closed)
            await driver.close()

        self.loop.run_until_complete(go())

    def test_submit(self):

        async def go():
            driver = Driver("http://localhost:8182/", self.loop)
            async with driver.get() as conn:
                async for msg in conn.submit("1 + 1"):
                    self.assertEqual(msg.data[0], 2)
            await driver.close()

        self.loop.run_until_complete(go())
