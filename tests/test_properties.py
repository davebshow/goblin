import asyncio
import unittest

from goblin.api import create_engine, Vertex, Edge, VertexProperty
from goblin.properties import Property, String


class TestVertexProperty(VertexProperty):
    notes = Property(String)


class TestVertex(Vertex):
    __label__ = 'test_vertex'
    name = Property(String, vertex_property=TestVertexProperty)
    address = Property(String)


class TestProperties(unittest.TestCase):

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)

    def tearDown(self):
        self.loop.close()

    def test_vertex_property(self):

        t = TestVertex()
        self.assertIsNone(t.name)
        t.name = 'leif'
        self.assertEqual(t.name._value, 'leif')
        self.assertIsNone(t.name.notes)
        t.name.notes = 'notes'
        self.assertEqual(t.name.notes, 'notes')
        t.name = ['leif', 'jon']
        self.assertEqual(t.name[0]._value, 'leif')
        self.assertEqual(t.name[1]._value, 'jon')
