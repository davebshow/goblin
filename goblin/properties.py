# Properties
class Property:

    def __init__(self, data_type):
        self._data_type = data_type

    @property
    def data_type(self):
        return self._data_type


class PropertyDescriptor:

    def __init__(self, data_type):
        self._data_type = data_type
        self._val = None

    def __get__(self, obj, objtype):
        print(self._data_type)
        return self._val

    def __set__(self, obj, val):
        self._val = self._data_type.validate(val)

    def __delete__(self, obj):
        self._val = None


# Data types
class String:

    @classmethod
    def validate(cls, val):
        return str(val)

    @classmethod
    def to_db(cls, val):
        return val

    @classmethod
    def to_ogm(cls, val):
        return val
