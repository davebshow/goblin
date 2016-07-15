import logging

import inflection

from goblin import abc
from goblin import mapper
from goblin import properties


logger = logging.getLogger(__name__)


class ElementMeta(type):
    """Metaclass for graph elements. Responsible for creating the
       the :py:class:`mapper.Mapping` object and replacing user defined
       :py:class:`property.Property` with
       :py:class:`property.PropertyDescriptor`"""
    def __new__(cls, name, bases, namespace, **kwds):
        if bases:
            namespace['__type__'] = bases[0].__name__.lower()
        if not namespace.get('__label__', None):
            namespace['__label__'] = inflection.underscore(name)
        props = {}
        new_namespace = {}
        for k, v in namespace.items():
            if isinstance(v, abc.BaseProperty):
                props[k] = v
                v = v.__descriptor__(k, v)
            new_namespace[k] = v
        new_namespace['__mapping__'] = mapper.create_mapping(namespace,
                                                             props)
        if name not in ('Element', 'Vertex', 'Edge', 'VertexProperty'
                        'GenericVertex', 'GenericEdge'):
            logger.warning("Creating new Element class {}: {}".format(
                name, new_namespace['__mapping__']))
        result = type.__new__(cls, name, bases, new_namespace)
        return result


class Element(metaclass=ElementMeta):
    pass


class Vertex(Element):
    """Base class for user defined Vertex classes"""
    pass


class GenericVertex(Vertex):
    pass


class Edge(Element):
    """Base class for user defined Edge classes"""

    def __init__(self, source=None, target=None):
        self._source = source
        self._target = target

    def getsource(self):
        return self._source

    def setsource(self, val):
        assert isinstance(val, Vertex) or val is None
        self._source = val

    def delsource(self):
        del self._source

    source = property(getsource, setsource, delsource)

    def gettarget(self):
        return self._target

    def settarget(self, val):
        assert isinstance(val, Vertex) or val is None
        self._target = val

    def deltarget(self):
        del self._target

    target = property(gettarget, settarget, deltarget)


class GenericEdge(Edge):
    pass


class VertexPropertyDescriptor:
    """Descriptor that validates user property input and gets/sets properties
       as instance attributes."""

    def __init__(self, name, vertex_property):
        self._name = '_' + name
        self._vertex_property = vertex_property.__class__
        self._data_type = vertex_property.data_type
        self._default = vertex_property.default

    def __get__(self, obj, objtype):
        if obj is None:
            return self._vertex_property
        default = self._default
        if default:
            default = self._data_type.validate(default)
            default = self._vertex_property(self._default)
        return getattr(obj, self._name, default)

    def __set__(self, obj, val):
        if isinstance(val, (list, tuple , set)):
            vertex_property = []
            for v in val:
                v = self._data_type.validate(v)
                vertex_property.append(
                    self._vertex_property(self._data_type, value=v))

        else:
            val = self._data_type.validate(val)
            vertex_property = self._vertex_property(self._data_type, value=val)
        setattr(obj, self._name, vertex_property)


class VertexProperty(Element, abc.BaseProperty):

    __descriptor__ = VertexPropertyDescriptor

    def __init__(self, data_type, *, value=None, default=None):
        if isinstance(data_type, type):
            data_type = data_type()
        self._data_type = data_type
        self._value = value
        self._default = default

    @property
    def default(self):
        self._default

    @property
    def data_type(self):
        return self._data_type

    @property
    def value(self):
        return self._value

    def __repr__(self):
        return '<{}(type={}, value={})'.format(self.__class__.__name__,
                                               self._data_type, self.value)
