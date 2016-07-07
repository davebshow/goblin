"""Classes to handle proerties and data type definitions"""
import abc
import logging

from goblin import mapper

logger = logging.getLogger(__name__)


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


class VertexPropertyDescriptor(PropertyDescriptor):
    """Descriptor that validates user property input and gets/sets properties
       as instance attributes."""

    def __get__(self, obj, objtype):
        if obj is None:
            return self._data_type
        default = self._default
        if default:
            default = self._data_type(self._default)
        return getattr(obj, self._name, default)

    def __set__(self, obj, val):
        if isinstance(val, (list, tuple , set)):
            vertex_property = []
            for v in val:
                v = self._data_type.__data_type__.validate(v)
                vertex_property.append(self._data_type(v))

        else:
            val = self._data_type.__data_type__.validate(val)
            vertex_property = self._data_type(val)
        setattr(obj, self._name, vertex_property)


class Property:
    """API class used to define properties. Replaced with
      :py:class:`PropertyDescriptor` by :py:class:`api.ElementMeta`."""

    descriptor = PropertyDescriptor

    def __init__(self, data_type, *, vertex_property=None, default=None):
        if isinstance(data_type, type):
            data_type = data_type()
        self._data_type = data_type
        if vertex_property:
            vertex_property.__data_type__ = data_type
        self._vertex_property = vertex_property
        self._default = default

    @property
    def data_type(self):
        return self._data_type

    @property
    def vertex_property(self):
        return self._vertex_property

    @property
    def default(self):
        return self._default


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
