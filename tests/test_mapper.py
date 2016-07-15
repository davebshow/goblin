import pytest

from goblin import exception, properties


def test_property_mapping(person, lives_in):
    db_name, data_type = person.__mapping__._properties['name']
    assert  db_name == 'person__name'
    assert isinstance(data_type, properties.String)
    db_name, data_type = person.__mapping__._properties['age']
    assert  db_name == 'person__age'
    assert isinstance(data_type, properties.Integer)
    db_name, data_type = lives_in.__mapping__._properties['notes']
    assert  db_name == 'lives_in__notes'
    assert isinstance(data_type, properties.String)


def test_label_creation(place, lives_in):
    assert place.__mapping__._label == 'place'
    assert lives_in.__mapping__._label == 'lives_in'


def test_mapper_func(place, knows):
    assert callable(place.__mapping__._mapper_func)
    assert callable(knows.__mapping__._mapper_func)


def test_getattr_getdbname(person, lives_in):
    db_name = person.__mapping__.name
    assert  db_name == 'person__name'
    db_name = person.__mapping__.age
    assert  db_name == 'person__age'
    db_name = lives_in.__mapping__.notes
    assert  db_name == 'lives_in__notes'


def test_getattr_doesnt_exist(person):
    with pytest.raises(exception.MappingError):
        db_name = person.__mapping__.doesnt_exits
