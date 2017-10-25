import abc
import logging

from gremlin_python.process.traversal import Cardinality

from goblin import manager, element, exception


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
                if not isinstance(v, element.VertexProperty):
                    vp = vertex_prop(data_type, card=card)
                    vp.value = self.validate(v)
                else:
                    vp = v
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
