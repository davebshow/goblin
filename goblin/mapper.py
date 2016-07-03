"""Helper functions and class to map between OGM Elements <-> DB Elements"""
import inflection


def props_generator(properties):
    for prop in properties:
        yield prop['ogm_name'], prop['db_name'], prop['data_type']


def map_props_to_db(element, mapping):
    """Convert OGM property names/values to DB property names/values"""
    property_tuples = []
    props = mapping.properties
    for ogm_name, db_name, data_type in props_generator(props):
        val = getattr(element, ogm_name, None)
        property_tuples.append((db_name, data_type.to_db(val)))
    return property_tuples


def map_vertex_to_ogm(result, element, mapping):
    """Map a vertex returned by DB to OGM vertex"""
    props = mapping.properties
    for ogm_name, db_name, data_type in props_generator(props):
        val = result['properties'].get(db_name, [{'value': None}])[0]['value']
        setattr(element, ogm_name, data_type.to_ogm(val))
    setattr(element, '__label__', result['label'])
    setattr(element, 'id', result['id'])
    return element


def map_edge_to_ogm(result, element, mapping):
    """Map an edge returned by DB to OGM edge"""
    props = mapping.properties
    for ogm_name, db_name, data_type in props_generator(props):
        val = result['properties'].get(db_name, None)
        setattr(element, ogm_name, data_type.to_ogm(val))
    setattr(element, '__label__', result['label'])
    setattr(element, 'id', result['id'])
    setattr(element.source, '__label__', result['inVLabel'])
    setattr(element.target, '__label__', result['outVLabel'])
    return element


# DB <-> OGM Mapping
def create_mapping(namespace, properties):
    """Constructor for :py:class:`Mapping`"""
    if namespace.get('__type__', None):
        return Mapping(namespace, properties)


class Mapping:
    """This class stores the information necessary to map between an
       OGM element and a DB element"""
    def __init__(self, namespace, properties):
        self._label = namespace.get('__label__', None) or self._create_label()
        self._type = namespace['__type__']
        self._properties = []
        self._map_properties(properties)

    @property
    def label(self):
        return self._label

    @property
    def properties(self):
        return self._properties

    def _create_label(self):
        return inflection.underscore(self.__class__.__name__)

    def _map_properties(self, properties):
        for name, prop in properties.items():
            data_type = prop.data_type
            db_name = '{}__{}'.format(self._label, name)
            mapping = {'ogm_name': name, 'db_name': db_name,
                       'data_type': data_type}
            self._properties.append(mapping)

    def __repr__(self):
        return '<{}(type={}, label={}, properties={})'.format(
            self.__class__.__name__,
            self._type,
            self._label,
            self._properties)
