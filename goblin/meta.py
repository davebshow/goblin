import logging

import inflection

from goblin import mapper
from goblin import properties


logger = logging.getLogger(__name__)


# Graph elements
class ElementMeta(type):
    """Metaclass for graph elements. Responsible for creating the
       the :py:class:`mapper.Mapping` object and replacing user defined
       :py:class:`property.Property` with
       :py:class:`property.PropertyDescriptor`"""
    def __new__(cls, name, bases, namespace, **kwds):
        if bases:
            namespace['__type__'] = inflection.underscore(bases[0].__name__)
        props = {}
        vertex_props = {}
        new_namespace = {}
        for k, v in namespace.items():
            if isinstance(v, properties.Property):
                props[k] = v
                v = properties.PropertyDescriptor(
                    k, v.data_type, default=v.default)
            else:
                element_type = getattr(v, '__type__', None)
                if element_type == 'vertex_property':
                    vertex_props[k] = v
                    vertex_property_class = v.__class__
                    vertex_property_class.__data_type__ = v.data_type
                    v = properties.VertexPropertyDescriptor(
                        k, vertex_property_class, default=v.default)
            new_namespace[k] = v
        new_namespace['__mapping__'] = mapper.create_mapping(namespace,
                                                             props,
                                                             vertex_props)
        logger.info("Creating new Element class {}: {}".format(
            name, new_namespace['__mapping__']))
        result = type.__new__(cls, name, bases, new_namespace)
        return result
