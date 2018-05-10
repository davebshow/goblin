import collections
import datetime
import logging

from goblin import properties

logger = logging.getLogger(__name__)


CARD_MAPPING = {'Cardinality.single': 'Cardinality.SINGLE',
                'Cardinality.list_':'Cardinality.LIST',
                'Cardinality.set_': 'Cardinality.SET'}


DATA_TYPE_MAPPING = {properties.Integer: 'Integer.class',
                     properties.Float: 'Float.class',
                     properties.String: 'String.class',
                     properties.Boolean: 'Boolean.class'}


prop_keys = {}


PropertyKey = collections.namedtuple('PropertyKey', ['name', 'data_type', 'card'])


async def create_schema(app, indices, cluster):
    client = await cluster.connect()
    schema_definition = get_schema(app, indices)
    start_time = datetime.datetime.now()
    logger.info("Processing schema....")
    resp = await client.submit(schema_definition)
    await resp.all()
    logger.info("Processed schema in {}".format(datetime.datetime.now() - start_time))


def get_schema(app, indices=None):
    if not indices:
        indices = []
    schema_definition = """graph.tx().rollback()
                           mgmt = graph.openManagement()\n"""
    for label, vertex in app.vertices.items():
        schema_definition += get_vertex_schema(label, vertex)
    schema_definition += "// Edge schema\n"
    for label, edge in app.edges.items():
        schema_definition += get_edge_schema(label, edge)
    # Need to register vertex props with app TODO Fix in Goblin

    schema_definition += get_indices_schema(indices)
    schema_definition += "mgmt.commit()"
    return schema_definition


def get_vertex_schema(label, vertex):
    vertex_schema = "// Schema for vertex label: {}\n".format(label)
    vertex_schema += "{} = mgmt.makeVertexLabel('{}').make()\n".format(label, label)
    mapping = vertex.__mapping__
    properties = vertex.__properties__
    for db_name, (ogm_name, _) in mapping.db_properties.items():
        prop = properties[ogm_name]

        # Get cardinality
        if hasattr(prop, 'cardinality'):
            card = str(prop.cardinality)
        else:
            card = 'Cardinality.single'
        mapped_card = CARD_MAPPING[card]

        # Get data type
        data_type = prop.data_type
        mapped_data_type = DATA_TYPE_MAPPING[data_type.__class__]
        prop_key = PropertyKey(db_name, mapped_data_type, mapped_card)
        if db_name in prop_keys:
            assert prop_key == prop_keys[db_name]
        else:
            prop_keys[db_name] = prop_key
        prop_key_string = "{} = mgmt.makePropertyKey('{}').dataType({}).cardinality({}).make()\n".format(
            prop_key.name, prop_key.name, prop_key.data_type, prop_key.card)
        vertex_schema += prop_key_string
    vertex_schema += "\n"
    return vertex_schema


def get_indices_schema(indices):
    indices_schema = "// Indices ...\n"
    for index in indices:
        indices_schema += "mgmt.buildIndex('by_{}', Vertex.class).addKey({}).buildCompositeIndex()\n".format(index, index)
    return indices_schema


def get_edge_schema(label, edge):
    edge_schema = "{} = mgmt.makeEdgeLabel('{}').multiplicity(SIMPLE).make()\n".format(label, label)
    #TODO edge prop keys
    return edge_schema
