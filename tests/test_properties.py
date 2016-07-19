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

import pytest


def test_set_change_property(person, lives_in):
    # vertex
    assert not person.name
    person.name = 'leif'
    assert person.name == 'leif'
    person.name = 'leifur'
    assert person.name == 'leifur'
    # edge
    assert not lives_in.notes
    lives_in.notes = 'notable'
    assert lives_in.notes == 'notable'
    lives_in.notes = 'more notable'
    assert lives_in.notes == 'more notable'


def test_property_default(knows):
    assert knows.notes == 'N/A'
    knows.notes = 'notable'
    assert knows.notes == 'notable'


def test_validation(person):
    person.age = 10
    with pytest.raises(Exception):
        person.age = 'hello'


def test_setattr_validation(person):
    setattr(person, 'age', 10)
    assert person.age == 10
    with pytest.raises(Exception):
        setattr(person, 'age', 'hello')


class TestString:

    def test_validation(self, string):
        assert string.validate(1) == '1'

    def test_to_db(self, string):
        assert string.to_db('hello') == 'hello'

    def test_to_ogm(self, string):
        assert string.to_ogm('hello') == 'hello'

    def test_initval_to_db(self, string_class):
        string = string_class('hello')
        assert string.to_db() == 'hello'


class TestInteger:

    def test_validation(self, integer):
        assert integer.validate('1') == 1
        with pytest.raises(Exception):
            integer.validate('hello')

    def test_to_db(self, integer):
        assert integer.to_db(1) == 1

    def test_to_ogm(self, integer):
        assert integer.to_db(1) == 1

    def test_initval_to_db(self, integer_class):
        integer = integer_class(1)
        assert integer.to_db() == 1
