# DictDiffer class modified from http://stackoverflow.com
# http://stackoverflow.com/questions/6027558/flatten-nested-python-dictionaries-compressing-keys

from itertools import *
from collections import *

class DictDiffer(object):
    """
    Calculate the difference between two dictionaries as:
    (1) List f items added
    (2) List of items removed
    (3) List of keys same in both but changed values
    """

    same = lambda x:x  # identity function
    add = lambda a,b:a+b
    _tuple = lambda x:(x,)  # python actually has coercion, avoid it like so

    def __init__(self, current_dict, past_dict):
        self.current_dict = self._pathify_dict(current_dict)
        self.past_dict = self._pathify_dict(past_dict)

        self.set_current = set(self.current_dict)
        self.set_past = set(self.past_dict)

        self.intersect = self.set_current.intersection(self.set_past)

    def _flatten_dict(self, dictionary, key_reducer=add, key_lift=_tuple, init=()):
        """Given a nested dict, returns a flattened dict with all nested keys as a tuple key"""

        def _flatten_iter(pairs, _key_accum=init):
            atoms = ((k,v) for k,v in pairs if not isinstance(v, Mapping))
            submaps = ((k,v) for k,v in pairs if isinstance(v, Mapping))
            def compress(k):
                return key_reducer(_key_accum, key_lift(k))
            return chain(((compress(k),v) for k,v in atoms),
                         *[_flatten_iter(submap.items(), compress(k)) for k, submap in submaps])
        return dict(_flatten_iter(dictionary.items()))

    def _pathify_dict(self, d):
        """Sets the tuple keys of dict d to 'tuple_val1/tuple_val2/.../tuple_valN'"""
        flat_dict = self._flatten_dict(d)
        return dict(("/".join(k), v) for k, v in flat_dict.iteritems())


    def added(self):
        return list(self.set_current - self.intersect)

    def removed(self):
        return list(self.set_past - self.intersect)

    def changed(self):
        return list(set(o for o in list(self.intersect) if
                    self.past_dict[o] != self.current_dict[o]))
