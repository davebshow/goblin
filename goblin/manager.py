"""Managers for multi cardinality vertex properties"""


class VertexPropertyManager:

    def __call__(self, val):
        results = []
        for v in self:
            if v.value == val:
                results.append(v)
        if len(results) == 1:
            results = results[0]
        elif not results:
            results = None
        return results


class ListVertexPropertyManager(list, VertexPropertyManager):

    def __init__(self, data_type, vertex_prop, card, obj):
        self._data_type = data_type
        self._vertex_prop = vertex_prop
        self._card = card
        list.__init__(self, obj)

    def append(self, val):
        val = self._data_type.validate(val)
        val = self._vertex_prop(self._data_type, val=val, card=self._card)
        super().append(val)


class SetVertexPropertyManager(set, VertexPropertyManager):

    def __init__(self, data_type, vertex_prop, card, obj):
        self._data_type = data_type
        self._vertex_prop = vertex_prop
        self._card = card
        set.__init__(self, obj)

    def add(self, val):
        val = self._data_type.validate(val)
        val = self._vertex_prop(self._data_type, val=val, card=self._card)
        super().add(val)
