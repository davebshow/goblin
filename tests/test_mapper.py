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

from goblin import exception, properties


def test_property_mapping(person, lives_in):
    db_name, data_type = person.__mapping__._ogm_properties['name']
    assert db_name == 'name'
    assert isinstance(data_type, properties.String)
    db_name, data_type = person.__mapping__._ogm_properties['age']
    assert db_name == 'custom__person__age'
    assert isinstance(data_type, properties.Integer)
    db_name, data_type = lives_in.__mapping__._ogm_properties['notes']
    assert db_name == 'notes'
    assert isinstance(data_type, properties.String)

    ogm_name, data_type = person.__mapping__._db_properties['name']
    assert ogm_name == 'name'
    assert isinstance(data_type, properties.String)
    ogm_name, data_type = person.__mapping__._db_properties['custom__person__age']
    assert ogm_name == 'age'
    assert isinstance(data_type, properties.Integer)
    ogm_name, data_type = lives_in.__mapping__._db_properties['notes']
    assert  ogm_name == 'notes'
    assert isinstance(data_type, properties.String)


def test_metaprop_mapping(place):
    place.historical_name = ['Iowa City']
    db_name, data_type = place.historical_name(
        'Iowa City').__mapping__._ogm_properties['notes']
    assert db_name == 'notes'
    assert isinstance(data_type, properties.String)
    db_name, data_type = place.historical_name(
        'Iowa City').__mapping__._ogm_properties['year']
    assert db_name == 'year'
    assert isinstance(data_type, properties.Integer)


def test_label_creation(place, lives_in):
    assert place.__mapping__._label == 'place'
    assert lives_in.__mapping__._label == 'lives_in'


def test_mapper_func(place, knows):
    assert callable(place.__mapping__._mapper_func)
    assert callable(knows.__mapping__._mapper_func)


def test_getattr_getdbname(person, lives_in):
    db_name = person.__mapping__.name
    assert  db_name == 'name'
    db_name = person.__mapping__.age
    assert  db_name == 'custom__person__age'
    db_name = lives_in.__mapping__.notes
    assert  db_name == 'notes'


def test_getattr_doesnt_exist(person):
    with pytest.raises(exception.MappingError):
        db_name = person.__mapping__.doesnt_exits
