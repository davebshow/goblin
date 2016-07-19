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

from goblin import element
from gremlin_python import process


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
