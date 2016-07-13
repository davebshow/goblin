import asyncio
import unittest

from goblin.app import create_app
from goblin.element import Vertex, Edge, VertexProperty
from goblin.properties import Property, String


class TestVertex(Vertex):
    __label__ = 'test_vertex'
    name = Property(String)
    notes = Property(String, default='N/A')


class TestEdge(Edge):
    __label__ = 'test_edge'
    notes = Property(String, default='N/A')


class TestEngine(unittest.TestCase):

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)

    def tearDown(self):
        self.loop.close()

    def test_add_vertex(self):

        app = self.loop.run_until_complete(
            create_app("http://localhost:8182/", self.loop))
        app.register(TestVertex)

        async def go():
            session = await app.session()
            leif = TestVertex()
            leif.name = 'leifur'
            leif.notes = 'superdev'
            session.add(leif)
            await session.flush()
            current = session._current[leif.id]
            self.assertEqual(current.name, 'leifur')
            self.assertEqual(current.notes, 'superdev')
            self.assertIs(leif, current)
            self.assertEqual(leif.id, current.id)
            await session.close()

        self.loop.run_until_complete(go())

    def test_update_vertex(self):

        app = self.loop.run_until_complete(
            create_app("http://localhost:8182/", self.loop))
        app.register(TestVertex)

        async def go():
            session = await app.session()
            leif = TestVertex()
            leif.name = 'leifur'
            session.add(leif)
            await session.flush()
            current = session._current[leif.id]
            self.assertEqual(current.name, 'leifur')
            self.assertEqual(current.notes, 'N/A')

            leif.name = 'leif'
            session.add(leif)
            await session.flush()
            new_current = session._current[leif.id]
            self.assertIs(current, new_current)
            self.assertEqual(new_current.name, 'leif')
            await session.close()


        self.loop.run_until_complete(go())

    def test_add_edge(self):

        app = self.loop.run_until_complete(
            create_app("http://localhost:8182/", self.loop))
        app.register(TestVertex, TestEdge)

        async def go():
            session = await app.session()
            leif = TestVertex()
            leif.name = 'leifur'
            jon = TestVertex()
            jon.name = 'jonathan'
            works_for = TestEdge()
            works_for.source = jon
            works_for.target = leif
            self.assertEqual(works_for.notes, 'N/A')
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
            await session.close()

        self.loop.run_until_complete(go())

    def test_update_edge(self):

        app = self.loop.run_until_complete(
            create_app("http://localhost:8182/", self.loop))
        app.register(TestVertex, TestEdge)

        async def go():
            session = await app.session()
            leif = TestVertex()
            leif.name = 'leifur'
            jon = TestVertex()
            jon.name = 'jonathan'
            works_for = TestEdge()
            works_for.source = jon
            works_for.target = leif
            session.add(leif, jon, works_for)
            await session.flush()
            current = session._current[works_for.id]
            self.assertEqual(works_for.notes, 'N/A')
            works_for.notes = 'zerofail'
            session.add(works_for)
            await session.flush()
            new_current = session._current[works_for.id]
            self.assertEqual(new_current.notes, 'zerofail')
            await session.close()

        self.loop.run_until_complete(go())


        self.loop.run_until_complete(go())

    def test_query_all(self):

        app = self.loop.run_until_complete(
            create_app("http://localhost:8182/", self.loop))
        app.register(TestVertex)

        async def go():
            session = await app.session()
            leif = TestVertex()
            leif.name = 'leifur'
            jon = TestVertex()
            jon.name = 'jonathan'
            session.add(leif, jon)
            await session.flush()
            results = []
            stream = await session.traversal(TestVertex).all()
            async for msg in stream:
                results.append(msg)
                print(len(results))
            self.assertEqual(len(session.current), 2)
            for result in results:
                self.assertIsInstance(result, Vertex)
            await session.close()

        self.loop.run_until_complete(go())

    def test_remove_vertex(self):

        app = self.loop.run_until_complete(
            create_app("http://localhost:8182/", self.loop))
        app.register(TestVertex, TestEdge)

        async def go():
            session = await app.session()
            leif = TestVertex()
            leif.name = 'leifur'
            session.add(leif)
            await session.flush()
            current = session._current[leif.id]
            self.assertIs(leif, current)
            await session.remove_vertex(leif)
            result = await session.get_vertex(leif)
            self.assertIsNone(result)
            self.assertEqual(len(list(session.current.items())), 0)
            await session.close()

        self.loop.run_until_complete(go())

    def test_remove_edge(self):

        app = self.loop.run_until_complete(
            create_app("http://localhost:8182/", self.loop))
        app.register(TestVertex, TestEdge)

        async def go():
            session = await app.session()
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
            self.assertIs(current, works_for)
            await session.remove_edge(works_for)
            result = await session.get_edge(works_for)
            self.assertIsNone(result)
            self.assertEqual(len(list(session.current.items())), 2)
            await session.close()

        self.loop.run_until_complete(go())

    def test_traversal(self):

        app = self.loop.run_until_complete(
            create_app("http://localhost:8182/", self.loop))
        app.register(TestVertex, TestEdge)

        async def go():
            session = await app.session()
            leif = TestVertex()
            leif.name = 'the one and only leifur'
            jon = TestVertex()
            jon.name = 'the one and only jonathan'
            works_for = TestEdge()
            works_for.source = jon
            works_for.target = leif
            works_for.notes = 'the one and only zerofail'
            session.add(leif, jon, works_for)
            await session.flush()
            result = await session.traversal(TestVertex).has(
                TestVertex.__mapping__.name, ('v1', 'the one and only leifur'))._in().all()
            async for msg in result:
                self.assertIs(msg, jon)
            result = await session.traversal(TestVertex).has(
                TestVertex.__mapping__.name, ('v1', 'the one and only jonathan')).out().all()
            async for msg in result:
                self.assertIs(msg, leif)
            await session.remove_vertex(leif)
            await session.remove_vertex(jon)
            await session.close()

        self.loop.run_until_complete(go())
