"""Classes to handle proerties and data type definitions"""
import logging

from goblin import abc

logger = logging.getLogger(__name__)


class PropertyDescriptor:
    """Descriptor that validates user property input and gets/sets properties
       as instance attributes."""

    def __init__(self, name, prop):
        self._name = '_' + name
        self._data_type = prop.data_type
        self._default = prop.default

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


class Property(abc.BaseProperty):
    """API class used to define properties. Replaced with
      :py:class:`PropertyDescriptor` by :py:class:`api.ElementMeta`."""

    __descriptor__ = PropertyDescriptor

    def __init__(self, data_type, *, default=None):
        if isinstance(data_type, type):
            data_type = data_type()
        self._data_type = data_type
        self._default = default

    @property
    def data_type(self):
        return self._data_type

    @property
    def default(self):
        return self._default


# Data types
class String(abc.DataType):
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
