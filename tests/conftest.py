import pytest
from goblin import create_app, driver, element, properties
from goblin.gremlin_python import process


class Person(element.Vertex):
    __label__ = 'person'
    name = properties.Property(properties.String)
    age = properties.Property(properties.Integer)


class Place(element.Vertex):
    name = properties.Property(properties.String)
    zipcode = properties.Property(properties.Integer)


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
    return 'http://localhost:{}/'.format(unused_tcp_port)


@pytest.fixture
def connection(gremlin_server, event_loop):
    conn = event_loop.run_until_complete(
        gremlin_server.open("http://localhost:8182/", event_loop))
    return conn


@pytest.fixture
def remote_graph(connection):
     translator = process.GroovyTranslator('g')
     return driver.AsyncRemoteGraph(translator, connection)


@pytest.fixture
def app(event_loop):
    app = event_loop.run_until_complete(
        create_app("http://localhost:8182/", event_loop))
    app.register(Person, Place, Knows, LivesIn)
    return app


@pytest.fixture
def session(event_loop, app):
    session = event_loop.run_until_complete(app.session())
    return session


# Instance fixtures
@pytest.fixture
def string():
    return properties.String()


@pytest.fixture
def integer():
    return properties.Integer()


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
def string_class():
    return properties.String


@pytest.fixture
def integer_class():
    return properties.Integer


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
