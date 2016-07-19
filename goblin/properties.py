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

"""Classes to handle proerties and data type definitions"""

import logging

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
        setattr(obj, self._name, self._data_type.validate(val))

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
    :param default: Default value for this property.
    """

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
            except ValueError as e:
                raise exception.ValidationError(
                    'Not a valid integer: {}'.format(val)) from e

    def to_db(self, val=None):
        return super().to_db(val=val)

    def to_ogm(self, val):
        return super().to_ogm(val)


class Float(abc.DataType):
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


class Bool(abc.DataType):
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
