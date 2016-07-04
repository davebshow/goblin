"""Classes to handle proerties and data type definitions"""
import abc


class Property:
    """API class used to define properties. Replaced with
      :py:class:`PropertyDescriptor` by :py:class:`api.ElementMeta`."""
    def __init__(self, data_type, *, default=None):
        if isinstance(data_type, type):
            data_type = data_type()
        self._data_type = data_type
        self._default = default

    @property
    def data_type(self):
        return self._data_type


class PropertyDescriptor:
    """Descriptor that validates user property input and gets/sets properties
       as instance attributes."""

    def __init__(self, name, data_type, *, default=None):
        self._name = '_' + name
        self._data_type = data_type
        self._default = default

    def __get__(self, obj, objtype):
        if obj is None:
            return self._data_type
        return getattr(obj, self._name, self._default)

    def __set__(self, obj, val):
        setattr(obj, self._name, self._data_type.validate(val))

    def __delete__(self, obj):
        # hmmm what is the best approach here
        attr = getattr(obj, self._name, None)
        if attr:
            del attr


class DataType(abc.ABC):

    @abc.abstractmethod
    def validate(self):
        raise NotImplementedError

    @abc.abstractmethod
    def to_db(self, val):
        return val

    @abc.abstractmethod
    def to_ogm(self, val):
        return val


# Data types
class String(DataType):
    """Simple string datatype"""

    def validate(self, val):
        if val is not None:
            try:
                return str(val)
            except Exception as e:
                raise Exception("Invalid") from e

    def to_db(self, val):
        return super().to_db(val)

    def to_ogm(self, val):
        return super().to_ogm(val)
