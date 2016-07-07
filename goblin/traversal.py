"""Class used to produce traversals"""
from goblin import gremlin_python
from goblin import mapper


class TraversalSource:
    """A wrapper for :py:class:gremlin_python.PythonGraphTraversalSource that
       generates commonly used traversals"""
    def __init__(self, translator):
        self._traversal_source = gremlin_python.PythonGraphTraversalSource(
            translator)
        self._binding = 0

    @property
    def g(self):
        return self.traversal_source

    @property
    def traversal_source(self):
        return self._traversal_source

    def remove_vertex(self, element):
        return self.g.V(element.id).drop()

    def remove_edge(self, element):
        return self.g.E(element.id).drop()

    def get_vertex_by_id(self, element):
        return self.g.V(element.id)

    def get_edge_by_id(self, element):
        return self.g.E(element.id)

    def add_vertex(self, element):
        props = mapper.map_props_to_db(element, element.__mapping__)
        traversal = self.g.addV(element.__mapping__.label)
        return self._add_properties(traversal, props)

    def add_edge(self, element):
        props = mapper.map_props_to_db(element, element.__mapping__)
        traversal = self.g.V(element.source.id)
        traversal = traversal.addE(element.__mapping__._label)
        traversal = traversal.to(self.g.V(element.target.id))
        return self._add_properties(traversal, props)

    def update_vertex(self, element):
        props = mapper.map_props_to_db(element, element.__mapping__)
        traversal = self.g.V(element.id)
        return self._add_properties(traversal, props)

    def update_edge(self, element):
        props = mapper.map_props_to_db(element, element.__mapping__)
        traversal = self.g.E(element.id)
        return self._add_properties(traversal, props)

    def _add_properties(self, traversal, props):
        for k, v in props:
            if v:
                traversal = traversal.property(
                    ('k' + str(self._binding), k),
                    ('v' + str(self._binding), v))
                self._binding += 1
        self._binding = 0
        return traversal
