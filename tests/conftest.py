# Copyright 2016 David M. Brown
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
from aiogremlin.gremlin_python.process.traversal import Cardinality
from goblin import Goblin, driver, element, properties
from goblin.driver import (
    Connection, DriverRemoteConnection, GraphSONMessageSerializer)
from goblin.provider import TinkerGraph


def pytest_generate_tests(metafunc):
    if 'cluster' in metafunc.fixturenames:
        metafunc.parametrize("cluster", ['c1', 'c2'], indirect=True)


def pytest_addoption(parser):
    parser.addoption('--provider', default='tinkergraph',
                     choices=('tinkergraph', 'dse',))
    parser.addoption('--gremlin-host', default='localhost')
    parser.addoption('--gremlin-port', default='8182')


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
        properties.String, card=Cardinality.list_)


class Place(element.Vertex):
    name = properties.Property(properties.String)
    zipcode = properties.Property(properties.Integer)
    historical_name = HistoricalName(properties.String, card=Cardinality.list_)
    important_numbers = element.VertexProperty(
        properties.Integer, card=Cardinality.set_)


class Inherited(Person):
    pass


class Knows(element.Edge):
    __label__ = 'knows'
    notes = properties.Property(properties.String, default='N/A')


class LivesIn(element.Edge):
    notes = properties.Property(properties.String)


@pytest.fixture
def provider(request):
    provider = request.config.getoption('provider')
    if provider == 'tinkergraph':
        return TinkerGraph
    elif provider == 'dse':
        try:
            import goblin_dse
        except ImportError:
            raise RuntimeError("Couldn't run tests with DSEGraph provider: the goblin_dse package "
                               "must be installed")
        else:
            return goblin_dse.DSEGraph


@pytest.fixture
def aliases(request):
    if request.config.getoption('provider') == 'tinkergraph':
        return {'g': 'g'}
    elif request.config.getoption('provider') == 'dse':
        return {'g': 'testgraph.g'}


@pytest.fixture
def gremlin_server():
    return driver.GremlinServer


@pytest.fixture
def unused_server_url(unused_tcp_port):
    return 'http://localhost:{}/gremlin'.format(unused_tcp_port)


@pytest.fixture
def gremlin_host(request):
    return request.config.getoption('gremlin_host')


@pytest.fixture
def gremlin_port(request):
    return request.config.getoption('gremlin_port')


@pytest.fixture
def gremlin_url(gremlin_host, gremlin_port):
    return "http://{}:{}/gremlin".format(gremlin_host, gremlin_port)


@pytest.fixture
def cluster(request, gremlin_host, gremlin_port, event_loop, provider, aliases):
    if request.param == 'c1':
        cluster = driver.Cluster(
            event_loop,
            hosts=[gremlin_host],
            port=gremlin_port,
            aliases=aliases,
            provider=provider
        )
    elif request.param == 'c2':
        cluster = driver.Cluster(
            event_loop,
            hosts=[gremlin_host],
            port=gremlin_port,
            aliases=aliases,
            provider=provider
        )
    return cluster


@pytest.fixture
def connection(gremlin_url, event_loop, provider):
    try:
        conn = event_loop.run_until_complete(
            driver.Connection.open(
                gremlin_url, event_loop,
                message_serializer=GraphSONMessageSerializer,
                provider=provider
            ))
    except OSError:
        pytest.skip('Gremlin Server is not running')
    return conn


@pytest.fixture
def remote_connection(event_loop, gremlin_url):
    try:
        remote_conn = event_loop.run_until_complete(
            DriverRemoteConnection.open(gremlin_url, 'g'))
    except OSError:
        pytest.skip('Gremlin Server is not running')
    else:
        return remote_conn


@pytest.fixture
def connection_pool(gremlin_url, event_loop, provider):
    return driver.ConnectionPool(
        gremlin_url, event_loop, None, '', '', 4, 1, 16,
        64, None, driver.GraphSONMessageSerializer, provider=provider)


@pytest.fixture
def remote_graph():
     return driver.Graph()


@pytest.fixture
def app(gremlin_host, gremlin_port, event_loop, provider, aliases):
    app = event_loop.run_until_complete(
        Goblin.open(event_loop, provider=provider, aliases=aliases, hosts=[gremlin_host],
                    port=gremlin_port))

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
def inherited_class():
    return Inherited


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
