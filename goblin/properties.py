"""Classes to handle proerties and data type definitions"""


class Property:
    """API class used to define properties. Replaced with
      :py:class:`PropertyDescriptor` by :py:class:`api.ElementMeta`."""
    def __init__(self, data_type, *, initval=None):
        self._data_type = data_type
        self._initval = initval

    @property
    def data_type(self):
        return self._data_type


class PropertyDescriptor:
    """Descriptor that validates user property input."""

    def __init__(self, name, data_type, *, initval=None):
        self._name = '_' + name
        self._data_type = data_type
        self._initval = initval

    def __get__(self, obj, objtype):
        print(self._data_type)
        if obj is None:
            return self
        return getattr(obj, self._name, self._initval)

    def __set__(self, obj, val):
        setattr(obj, self._name, self._data_type.validate(val))

    def __delete__(self, obj):
        self._val = None


# Data types
class String:
    """Simple string datatype"""
    @classmethod
    def validate(cls, val):
        if val:
            try:
                return str(val)
            except Exception as e:
                raise Exception("Invalid") from e

    @classmethod
    def to_db(cls, val):
        return val

    @classmethod
    def to_ogm(cls, val):
        return val
