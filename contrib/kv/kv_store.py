
"""Naive in-memory KV store definition."""


from collections import defaultdict

from wade import chain


class Store(chain.Store):
    def __init__(self):
        self._data = defaultdict(dict)
        self._seq_map = defaultdict(int)

    def serialize_obj(self, obj_id):
        return [self._data[obj_id], self._seq_map[obj_id]]

    def deserialize_obj(self, obj_id, value):
        self._data[obj_id], self._seq_map[obj_id] = value

    def max_seq(self, obj_id):
        return self._seq_map[obj_id]

    @chain.update_op
    def SET(self, obj_id, obj_seq, args):
        k, v = args['k'], args['v']

        self._data[obj_id][k] = v
        self._seq_map[obj_id] = obj_seq

        return 'OK'

    @chain.query_op
    def GET(self, obj_id, obj_seq, args):
        k = args['k']
        return self._data[obj_id].get(k)
