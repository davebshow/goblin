import collections
try:
    import ujson as json
except ImportError:
    import json

from gremlin_python.structure.io import graphsonV3d0 as graphson
from goblin.element import Vertex, Edge, VertexProperty
from goblin.manager import ListVertexPropertyManager


writer = graphson.GraphSONWriter()


AdjList = collections.namedtuple("AdjList", "vertex inE outE")

vp_id = 10


def dump(fpath, *adj_lists, mode="w"):
    """Convert Goblin elements to GraphSON"""
    with open(fpath, mode) as f:
        for adj_list in adj_lists:
            dumped = dumps(adj_list)
            f.write(dumped + '\n')


def dumps(adj_list):
    """Convert Goblin elements to GraphSON"""
    vertex = _prep_vertex(adj_list.vertex)
    if adj_list.inE:
        for inE in adj_list.inE:
            prepped = _prep_edge(inE, "inV")
            label = inE.__label__
            vertex.setdefault("inE", {})
            vertex["inE"].setdefault(label, [])
            vertex["inE"][label].append(prepped)
    if adj_list.outE:
        for outE in adj_list.outE:
            prepped = _prep_edge(outE, "outV")
            label = outE.__label__
            vertex.setdefault("outE", {})
            vertex["outE"].setdefault(label, [])
            vertex["outE"][label].append(prepped)
    return json.dumps(vertex)


def _prep_edge(e, t):
    if t == 'inV':
        other = "outV"
        other_id = e.source.id
    elif t == 'outV':
        other = "inV"
        other_id = e.target.id
    else:
        raise RuntimeError('Invalid edge type')
    edge = {
        "id": {
            "@type": "g:Int64",
            "@value": e.id,

        },
        other: {
            "@type": "g:Int64",
            "@value": other_id,
        }
    }
    for db_name, (ogm_name, _) in e.__mapping__.db_properties.items():
        data = writer.toDict(getattr(e, ogm_name))
        if not data:
            continue
        edge.setdefault("properties", {})
        edge["properties"][db_name] = data

    return edge


def _prep_vertex(v):
    global vp_id
    mapping = v.__mapping__
    properties = v.__properties__
    vertex = {
            "id": {
                "@type": "g:Int64",
                "@value": v.id
            },
            "label": v.__label__
    }

    for db_name, (ogm_name, _) in mapping.db_properties.items():
        prop = properties[ogm_name]
        vertex.setdefault("properties", {})
        if isinstance(prop, VertexProperty):
            prop = getattr(v, ogm_name)
            if isinstance(prop, ListVertexPropertyManager):
                for p in prop:
                    value = p.value
                    vp = _prep_vp(p, value, v, db_name)
                    vp_id += 1
                    vertex["properties"].setdefault(db_name, [])
                    vertex["properties"][db_name].append(vp)
                continue
            else:
                if not prop:
                    continue
                value = prop.value
        else:
            value = getattr(v, ogm_name)
        vp = _prep_vp(prop, value, v, db_name)
        vp_id += 1
        vertex["properties"].setdefault(db_name, [])
        vertex["properties"][db_name].append(vp)
    return vertex


def _prep_vp(prop, value, v, db_name):
    vp = {
            "id": {
                "@type": "g:Int64",
                "@value": vp_id
            },
            "value": writer.toDict(value),
    }
    if isinstance(prop, VertexProperty):
        for db_name, (ogm_name, _) in prop.__mapping__.db_properties.items():
            data = writer.toDict(getattr(prop, ogm_name))
            if not data:
                continue
            vp.setdefault("properties", {})
            vp["properties"][db_name] = data
    return vp
