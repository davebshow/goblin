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

import abc
import logging

from goblin import cardinality, manager, exception


logger = logging.getLogger(__name__)


class DataType(abc.ABC):
    """
    Abstract base class for Goblin Data Types. All custom data types should
    inherit from :py:class:`DataType`.
    """
    def __init__(self, val=None):
        self._val = val

    @abc.abstractmethod
    def validate(self, val):
        """Validate property value"""
        raise NotImplementedError

    @abc.abstractmethod
    def to_db(self, val=None):
        """
        Convert property value to db compatible format. If no value passed, try
        to use default bound value
        """
        if val is None:
            val = self._val
        return val

    @abc.abstractmethod
    def to_ogm(self, val):
        """Convert property value to a Python compatible format"""
        return val

    def validate_vertex_prop(self, val, card, vertex_prop, data_type):
        if card == cardinality.Cardinality.list:
            if isinstance(val, list):
                val = val
            elif isinstance(val, (set, tuple)):
                val = list(val)
            else:
                val = [val]
            val = manager.ListVertexPropertyManager(
                data_type, vertex_prop, card,
                [vertex_prop(data_type, val=self.validate(v), card=card)
                 for v in val])
        elif card == cardinality.Cardinality.set:
            if isinstance(val, set):
                val = val
            elif isinstance(val, (list, tuple)):
                val = set(val)
            else:
                val = set([val])
            val = manager.SetVertexPropertyManager(
                data_type, vertex_prop, card,
                {vertex_prop(data_type, val=self.validate(v), card=card)
                 for v in val})
        else:
            val = vertex_prop(data_type, val=self.validate(val))
        return val


class BaseProperty:
    """Abstract base class that implements the property interface"""
    @property
    def data_type(self):
        raise NotImplementedError
