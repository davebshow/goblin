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


class TestInteger:

    def test_validation(self, integer):
        assert integer.validate('1') == 1
        with pytest.raises(Exception):
            integer.validate('hello')

    def test_to_db(self, integer):
        assert integer.to_db(1) == 1

    def test_to_ogm(self, integer):
        assert integer.to_db(1) == 1
