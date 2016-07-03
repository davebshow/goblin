import asyncio
import unittest

from goblin.api import create_engine, Vertex, Edge
from goblin.properties import Property, String


class TestVertex(Vertex):
    __label__ = 'test_vertex'
    name = Property(String)


class TestEdge(Edge):
    __label__ = 'test_edge'
    notes = Property(String)


class TestEngine(unittest.TestCase):

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)

    def tearDown(self):
        self.loop.close()

    def test_add_vertex(self):

        async def go():
            engine = await create_engine("http://localhost:8182/", self.loop)
            session = engine.session()
            leif = TestVertex()
            leif.name = 'leifur'
            session.add(leif)
            await session.flush()
            current = session._current[leif.id]
            self.assertEqual(current.name, 'leifur')
            self.assertIs(leif, current)
            self.assertEqual(leif.id, current.id)
            await engine.close()

        self.loop.run_until_complete(go())

    def test_add_edge(self):

        async def go():
            engine = await create_engine("http://localhost:8182/", self.loop)
            session = engine.session()
            leif = TestVertex()
            leif.name = 'leifur'
            jon = TestVertex()
            jon.name = 'jonathan'
            works_for = TestEdge()
            works_for.source = jon
            works_for.target = leif
            works_for.notes = 'zerofail'
            session.add(leif, jon, works_for)
            await session.flush()
            current = session._current[works_for.id]
            self.assertEqual(current.notes, 'zerofail')
            self.assertIs(current, works_for)
            self.assertEqual(current.id, works_for.id)
            self.assertIs(leif, current.target)
            self.assertEqual(leif.id, current.target.id)
            self.assertIs(jon, current.source)
            self.assertEqual(jon.id, current.source.id)
            await engine.close()

        self.loop.run_until_complete(go())

    def test_query_all(self):

        async def go():
            engine = await create_engine("http://localhost:8182/", self.loop)
            session = engine.session()
            leif = TestVertex()
            leif.name = 'leifur'
            jon = TestVertex()
            jon.name = 'jonathan'
            session.add(leif, jon)
            await session.flush()
            results = []
            stream = await session.query(TestVertex).all()
            async for msg in stream:
                results += msg.data
                print(len(results))
            self.assertTrue(len(results) > 1)
            await stream.close()
            await engine.close()

        self.loop.run_until_complete(go())
