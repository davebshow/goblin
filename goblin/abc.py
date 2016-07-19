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
        """Convert property value to db compatible format"""
        if not val:
            val = self._val
        return val

    @abc.abstractmethod
    def to_ogm(self, val):
        """Convert property value to a Python compatible format"""
        try:
            self.validate(val)
        except exception.ValidationError:
            logger.warning(
                "DB val {} Fails OGM validation for {}".format(val, self))
        return val


class BaseProperty:
    """Abstract base class that implements the property interface"""
    @property
    def data_type(self):
        raise NotImplementedError
