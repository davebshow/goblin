import pytest

import goblin
from goblin import element


@pytest.mark.asyncio
async def test_register_from_module(app):
    import register_models
    app.register_from_module(register_models)
    vertices, edges = app._vertices.values(), app._edges.values()
    assert register_models.TestRegisterVertex1 in vertices
    assert register_models.TestRegisterVertex2 in vertices
    assert register_models.TestRegisterEdge1 in edges
    assert register_models.TestRegisterEdge2 in edges
    await app.close()


@pytest.mark.asyncio
async def test_register_from_module_string(app):
    app.register_from_module('register_models', package=__package__)
    vertices, edges = app._vertices.values(), app._edges.values()
    import register_models
    assert register_models.TestRegisterVertex1 in vertices
    assert register_models.TestRegisterVertex2 in vertices
    assert register_models.TestRegisterEdge1 in edges
    assert register_models.TestRegisterEdge2 in edges
    await app.close()


@pytest.mark.asyncio
async def test_registry(app, person, place, knows, lives_in):
    assert len(app.vertices) == 2
    assert len(app.edges) == 2
    assert person.__class__ == app.vertices['person']
    assert place.__class__ == app.vertices['place']
    assert knows.__class__ == app.edges['knows']
    assert lives_in.__class__ == app.edges['lives_in']
    await app.close()


@pytest.mark.asyncio
async def test_registry_defaults(app):
    vertex = app.vertices['unregistered']
    assert isinstance(vertex(), element.Vertex)
    edge = app.edges['unregistered']
    assert isinstance(edge(), element.Edge)
    await app.close()


@pytest.mark.asyncio
async def test_aliases(app, aliases):
    session = await app.session()
    assert session._remote_connection._client.aliases == aliases
    await app.close()
