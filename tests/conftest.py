# Copyright 2016 ZEROFAIL
#
# This file is part of Goblin.
#
# Goblin is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Goblin is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with Goblin.  If not, see <http://www.gnu.org/licenses/>.
import asyncio
import pytest
from goblin import Goblin, driver, element, properties, Cardinality
from goblin.driver import pool, serializer
from gremlin_python import process


def pytest_generate_tests(metafunc):
    if 'cluster' in metafunc.fixturenames:
        metafunc.parametrize("cluster", ['c1', 'c2'], indirect=True)


class HistoricalName(element.VertexProperty):
    notes = properties.Property(properties.String)
    year = properties.Property(properties.Integer)  # this is dumb but handy


class Person(element.Vertex):
    __label__ = 'person'
    name = properties.Property(properties.String)
    age = properties.Property(properties.Integer,
                              db_name='custom__person__age')
    birthplace = element.VertexProperty(properties.String)
    nicknames = element.VertexProperty(
        properties.String, card=Cardinality.list)


class Place(element.Vertex):
    name = properties.Property(properties.String)
    zipcode = properties.Property(properties.Integer)
    historical_name = HistoricalName(properties.String, card=Cardinality.list)
    important_numbers = element.VertexProperty(
        properties.Integer, card=Cardinality.set)


class Knows(element.Edge):
    __label__ = 'knows'
    notes = properties.Property(properties.String, default='N/A')


class LivesIn(element.Edge):
    notes = properties.Property(properties.String)


@pytest.fixture
def gremlin_server():
    return driver.GremlinServer


@pytest.fixture
def unused_server_url(unused_tcp_port):
    return 'http://localhost:{}/gremlin'.format(unused_tcp_port)


@pytest.fixture
def connection(event_loop):
    conn = event_loop.run_until_complete(
        driver.Connection.open(
            "http://localhost:8182/gremlin", event_loop,
            message_serializer=serializer.GraphSONMessageSerializer))
    return conn


@pytest.fixture
def connection_pool(event_loop):
    return pool.ConnectionPool(
        "http://localhost:8182/gremlin", event_loop, None, '', '', 4, 1, 16,
        64, None, serializer.GraphSONMessageSerializer)


@pytest.fixture
def cluster(request, event_loop):
    if request.param == 'c1':
        cluster = driver.Cluster(
            event_loop,
            message_serializer=serializer.GraphSONMessageSerializer)
    elif request.param == 'c2':
        cluster = driver.Cluster(
            event_loop,
            message_serializer=serializer.GraphSON2MessageSerializer)
    return cluster


@pytest.fixture
def remote_graph():
     return driver.AsyncGraph()


@pytest.fixture
def app(request, event_loop):
    app = event_loop.run_until_complete(
        Goblin.open(event_loop, aliases={'g': 'g'}))

    app.register(Person, Place, Knows, LivesIn)
    return app


# Instance fixtures
@pytest.fixture
def string():
    return properties.String()


@pytest.fixture
def integer():
    return properties.Integer()


@pytest.fixture
def flt():
    return properties.Float()


@pytest.fixture
def boolean():
    return properties.Boolean()


@pytest.fixture
def historical_name():
    return HistoricalName()


@pytest.fixture
def person():
    return Person()


@pytest.fixture
def place():
    return Place()


@pytest.fixture
def knows():
    return Knows()


@pytest.fixture
def lives_in():
    return LivesIn()


@pytest.fixture
def place_name():
    return PlaceName()


# Class fixtures
@pytest.fixture
def cluster_class(event_loop):
    return driver.Cluster


@pytest.fixture
def string_class():
    return properties.String


@pytest.fixture
def integer_class():
    return properties.Integer


@pytest.fixture
def historical_name_class():
    return HistoricalName


@pytest.fixture
def person_class():
    return Person


@pytest.fixture
def place_class():
    return Place


@pytest.fixture
def knows_class():
    return Knows


@pytest.fixture
def lives_in_class():
    return LivesIn


@pytest.fixture
def place_name_class():
    return PlaceName


@pytest.fixture
def string_class():
    return properties.String


@pytest.fixture
def integer_class():
    return properties.Integer


@pytest.fixture
def flt_class():
    return properties.Float


@pytest.fixture
def boolean_class():
    return properties.Boolean
