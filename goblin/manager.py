"""Managers for multi cardinality vertex properties"""


class VertexPropertyManager:
    def __init__(self, data_type, vertex_prop, card):
        self._data_type = data_type
        self._vertex_prop = vertex_prop
        self._card = card
        self._mapper_func = vertex_prop.__mapping__.mapper_func

    @property
    def mapper_func(self):
        return self._mapper_func

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
        VertexPropertyManager.__init__(self, data_type, vertex_prop, card)
        list.__init__(self, obj)
        self._vp_map = {}

    @property
    def vp_map(self):
        return self._vp_map

    def append(self, val):
        vp = self._vertex_prop(self._data_type, card=self._card)
        vp.value = self._data_type.validate(val)
        super().append(vp)


class SetVertexPropertyManager(set, VertexPropertyManager):
    def __init__(self, data_type, vertex_prop, card, obj):
        VertexPropertyManager.__init__(self, data_type, vertex_prop, card)
        set.__init__(self, obj)

    def add(self, val):
        vp = self._vertex_prop(self._data_type, card=self._card)
        vp.value = self._data_type.validate(val)
        super().add(vp)
