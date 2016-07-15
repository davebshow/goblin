from goblin import element
from goblin.gremlin_python import process


def test_registry(app, person, place, knows, lives_in):
    assert len(app.vertices) ==  2
    assert len(app.edges) == 2
    assert person.__class__ == app.vertices['person']
    assert place.__class__ == app.vertices['place']
    assert knows.__class__ == app.edges['knows']
    assert lives_in.__class__ == app.edges['lives_in']


def test_registry_defaults(app):
    vertex = app.vertices['unregistered']
    assert isinstance(vertex(), element.Vertex)
    edge = app.edges['unregistered']
    assert isinstance(edge(), element.Edge)


def test_features(app):
    assert app._features


def test_translator(app):
    assert isinstance(app.translator, process.GroovyTranslator)
