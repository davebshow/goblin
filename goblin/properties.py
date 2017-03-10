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

"""Classes to handle properties and data type definitions"""

import logging

from aiogremlin.gremlin_python.statics import long

from goblin import abc, exception


logger = logging.getLogger(__name__)


class PropertyDescriptor:
    """
    Descriptor that validates user property input and gets/sets properties
    as instance attributes. Not instantiated by user.
    """

    def __init__(self, name, prop):
        self._prop_name = name
        self._name = '_' + name
        self._data_type = prop.data_type
        self._default = prop.default

    def __get__(self, obj, objtype):
        if obj is None:
            return getattr(objtype.__mapping__, self._prop_name)
        return getattr(obj, self._name, self._default)

    def __set__(self, obj, val):
        val = self._data_type.validate(val)
        setattr(obj, self._name, val)

    def __delete__(self, obj):
        # hmmm what is the best approach here
        attr = getattr(obj, self._name, None)
        if attr:
            del attr


class Property(abc.BaseProperty):
    """
    API class used to define properties. Replaced with
    :py:class:`PropertyDescriptor` by :py:class:`goblin.element.ElementMeta`.

    :param goblin.abc.DataType data_type: Str or class of data type
    :param str db_name: User defined custom name for property in db
    :param default: Default value for this property.
    """

    __descriptor__ = PropertyDescriptor

    def __init__(self, data_type, *, db_name=None, default=None):
        if isinstance(data_type, type):
            data_type = data_type()
        self._data_type = data_type
        self._db_name = db_name
        self._default = default

    @property
    def data_type(self):
        return self._data_type

    @property
    def db_name(self):
        return self._db_name

    @property
    def default(self):
        return self._default


class IdPropertyDescriptor:

    def __init__(self, name, prop):
        assert name == 'id', 'ID properties must be named "id"'
        self._data_type = prop.data_type
        self._name = '_' + name
        self._serializer = prop.serializer

    def __get__(self, obj, objtype=None):
        if obj is None:
            raise exception.ElementError(
                "Only instantiated elements have ID property")
        return obj._id

    def __set__(self, obj, val):
        if self._serializer:
            val = self._serializer(val)
        val = self._data_type.validate(val)
        setattr(obj, self._name, val)


def default_id_serializer(val):
    if isinstance(val, int):
        val = long(val)
    return val


class IdProperty(abc.BaseProperty):

    __descriptor__ = IdPropertyDescriptor

    def __init__(self, data_type, *, serializer=None):
        if not serializer:
            serializer = default_id_serializer
        if isinstance(data_type, type):
            data_type = data_type()
        self._data_type = data_type
        self._serializer = serializer

    @property
    def data_type(self):
        return self._data_type

    @property
    def serializer(self):
        return self._serializer


# Data types
class Generic(abc.DataType):

    def validate(self, val):
        return super().validate(val)

    def to_db(self, val=None):
        return super().to_db(val=val)

    def to_ogm(self, val):
        return super().to_ogm(val)


class String(abc.DataType):
    """Simple string datatype"""

    def validate(self, val):
        if val is not None:
            try:
                return str(val)
            except ValueError as e:
                raise exception.ValidationError(
                    'Not a valid string: {}'.format(val)) from e

    def to_db(self, val=None):
        return super().to_db(val=val)

    def to_ogm(self, val):
        return super().to_ogm(val)


class Integer(abc.DataType):
    """Simple integer datatype"""

    def validate(self, val):
        if val is not None:
            try:
                return int(val)
            except (ValueError, TypeError) as e:
                raise exception.ValidationError(
                    'Not a valid integer: {}'.format(val)) from e

    def to_db(self, val=None):
        return super().to_db(val=val)

    def to_ogm(self, val):
        return super().to_ogm(val)


class Float(abc.DataType):
    """Simple float datatype"""
    def validate(self, val):
        try:
            val = float(val)
        except ValueError:
            raise exception.ValidationError(
                "Not a valid float: {}".format(val)) from e
        return val

    def to_db(self, val=None):
        return super().to_db(val=val)

    def to_ogm(self, val):
        return super().to_ogm(val)


class Boolean(abc.DataType):
    """Simple boolean datatype"""
    def validate(self, val):
        try:
            val = bool(val)
        except ValueError:
            raise exception.ValidationError(
                "Not a valid boolean: {val}".format(val)) from e
        return val

    def to_db(self, val=None):
        return super().to_db(val=val)

    def to_ogm(self, val):
        return super().to_ogm(val)
