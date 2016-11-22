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

import pytest

import goblin
from goblin import element
from goblin.driver import serializer
from gremlin_python import process


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
    assert len(app.vertices) ==  2
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
    assert session._conn._aliases == aliases
    await app.close()
