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

import logging

import inflection

from aiogremlin.gremlin_python.process.traversal import Cardinality

from goblin import abc, exception, mapper, properties


logger = logging.getLogger(__name__)


class ElementMeta(type):
    """
    Metaclass for graph elements. Responsible for creating the
    :py:class:`Mapping<mapper.Mapping>` object and replacing user defined
    :py:class:`property.Property` with :py:class:`property.PropertyDescriptor`.
    """
    def __new__(cls, name, bases, namespace, **kwds):
        props = {}
        if name == 'VertexProperty':
            element_type = name.lower()
        elif bases:
            element_type = bases[0].__type__
            if element_type not in ['vertex', 'edge']:
                element_type = bases[0].__name__.lower()
            for base in bases:
                base_props = getattr(base, '__properties__', {})
                props.update(base_props)
        else:
            element_type = name.lower()
        namespace['__type__'] = element_type
        if not namespace.get('__label__', None):
            namespace['__label__'] = inflection.underscore(name)
        new_namespace = {}
        props.pop('id', None)
        for k, v in namespace.items():
            if isinstance(v, abc.BaseProperty):
                if element_type == 'edge' and hasattr(v, 'cardinality'):
                    raise exception.MappingError(
                        'Edge property cannot have set/list cardinality')
                props[k] = v
                v = v.__descriptor__(k, v)
            new_namespace[k] = v
        new_namespace['__mapping__'] = mapper.create_mapping(namespace,
                                                             props)
        new_namespace['__properties__'] = props
        result = type.__new__(cls, name, bases, new_namespace)
        return result


class Element(metaclass=ElementMeta):
    """Base class for classes that implement the Element property interface"""
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            if not (hasattr(self, key) and
                    isinstance(getattr(self, key), properties.PropertyDescriptor)):
                raise AssertionError(
                    "No such property: {} for element {}".format(key, self.__class__.__name__))
            setattr(self, key, value)

    id = properties.IdProperty(properties.Generic)


class Vertex(Element):
    """Base class for user defined Vertex classes"""
    pass


class GenericVertex(Vertex):
    """
    Class used to build vertices when user defined vertex class is not
    available. Generally not instantiated by end user.
    """
    pass


class Edge(Element):
    """
    Base class for user defined Edge classes.

    :param Vertex source: Source (outV) vertex
    :param Vertex target: Target (inV) vertex
    """
    def __init__(self, source=None, target=None):
        self.source = source
        self.target = target

    def getsource(self):
        return self._source

    def setsource(self, vertex):
        assert isinstance(vertex, Vertex) or vertex is None
        self._source = vertex

    def delsource(self):
        del self._source

    source = property(getsource, setsource, delsource)

    def gettarget(self):
        return self._target

    def settarget(self, vertex):
        assert isinstance(vertex, Vertex) or vertex is None
        self._target = vertex

    def deltarget(self):
        del self._target

    target = property(gettarget, settarget, deltarget)


class GenericEdge(Edge):
    """
    Class used to build edges when user defined edges class is not available.
    Generally not instantiated by end user.
    """
    pass

class VertexPropertyDescriptor:
    """
    Descriptor that validates user property input and gets/sets properties
    as instance attributes.
    """

    def __init__(self, name, vertex_property):
        self._prop_name = name
        self._name = '_' + name
        self._vertex_property = vertex_property.__class__
        self._data_type = vertex_property.data_type
        self._default = vertex_property.default
        self._cardinality = vertex_property._cardinality

    def __get__(self, obj, objtype):
        if obj is None:
            return getattr(objtype.__mapping__, self._prop_name)
        default = self._default
        if default:
            default = self._data_type.validate_vertex_prop(
                default, self._cardinality, self._vertex_property,
                self._data_type)
        return getattr(obj, self._name, default)

    def __set__(self, obj, val):
        if val is not None:
            val = self._data_type.validate_vertex_prop(
                val, self._cardinality, self._vertex_property, self._data_type)
        setattr(obj, self._name, val)


class VertexProperty(Vertex, abc.BaseProperty):
    """Base class for user defined vertex properties."""

    __descriptor__ = VertexPropertyDescriptor

    def __init__(self, data_type, *, default=None, db_name=None,
                 card=None):
        if isinstance(data_type, type):
            data_type = data_type()
        self._data_type = data_type
        self._default = default
        self._db_name = db_name
        self._val = None
        if card is None:
            card = Cardinality.single
        self._cardinality = card

    @property
    def default(self):
        self._default

    @property
    def data_type(self):
        return self._data_type

    def getvalue(self):
        return self._val

    def setvalue(self, val):
        self._val = val

    value = property(getvalue, setvalue)

    @property
    def db_name(self):
        return self._db_name

    @property
    def cardinality(self):
        return self._cardinality

    def __repr__(self):
        return '<{}(type={}, value={})'.format(self.__class__.__name__,
                                               self._data_type, self.value)
