import logging

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
            namespace['__type__'] = bases[0].__name__.lower()
        props = {}
        new_namespace = {}
        for k, v in namespace.items():
            if isinstance(v, properties.Property):
                props[k] = v
                if v.vertex_property:
                    v = properties.VertexPropertyDescriptor(
                        k, v.vertex_property, default=v.default)
                else:
                    v = properties.PropertyDescriptor(
                        k, v.data_type, default=v.default)
            new_namespace[k] = v
        new_namespace['__mapping__'] = mapper.create_mapping(namespace,
                                                             props)
        logger.warning("Creating new Element class {}: {}".format(
            name, new_namespace['__mapping__']))
        result = type.__new__(cls, name, bases, new_namespace)
        return result
