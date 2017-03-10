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

import abc
import logging

from aiogremlin.gremlin_python.process.traversal import Cardinality

from goblin import manager, exception


logger = logging.getLogger(__name__)


class DataType(abc.ABC):
    """
    Abstract base class for Goblin Data Types. All custom data types should
    inherit from :py:class:`DataType`.
    """
    def __init__(self, val=None):
        if val:
            val = self.validate(val)
        self._val = val

    @abc.abstractmethod
    def validate(self, val):
        """Validate property value"""
        return val

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
        if card == Cardinality.list_:
            if isinstance(val, list):
                val = val
            elif isinstance(val, (set, tuple)):
                val = list(val)
            else:
                val = [val]
            vertex_props = []
            for v in val:
                vp = vertex_prop(data_type, card=card)
                vp.value = self.validate(v)
                vertex_props.append(vp)
            val = manager.ListVertexPropertyManager(
                data_type, vertex_prop, card, vertex_props)
        elif card == Cardinality.set_:
            if isinstance(val, set):
                val = val
            elif isinstance(val, (list, tuple)):
                val = set(val)
            else:
                val = set([val])
            vertex_props = set([])
            for v in val:
                vp = vertex_prop(data_type, card=card)
                vp.value = self.validate(v)
                vertex_props.add(vp)
            val = manager.SetVertexPropertyManager(
                data_type, vertex_prop, card, vertex_props)
        else:
            vp = vertex_prop(data_type)
            vp.value = self.validate(val)
            val = vp
        return val


class BaseProperty:
    """Abstract base class that implements the property interface"""
    @property
    def data_type(self):
        raise NotImplementedError
