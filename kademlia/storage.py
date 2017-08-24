import time
from itertools import takewhile
import operator
from collections import OrderedDict


class IStorage:
    """
    Local storage for this node.
    """

    def __setitem__(self, key, value):
        """
        Set a key to the given value.
        """
        raise NotImplementedError

    def __getitem__(self, item):
        """
        Get the given key.  If item doesn't exist, raises C{KeyError}
        """
        raise NotImplementedError

    def get(self, key, default=None):
        """
        Get given key.  If not found, return default.
        """
        raise NotImplementedError

    def iter_items_older_than(self, seconds_old):
        """
        Return the an iterator over (key, value) tuples for items older than the given secondsOld.
        """
        raise NotImplementedError

    # def iteritems(self):
    #     """
    #     Get the iterator for this storage, should yield tuple of (key, value)
    #     """
    #     raise NotImplementedError


class ForgetfulStorage(IStorage):
    def __init__(self, ttl=604800):
        """
        By default, max age is a week.
        """
        self.data = OrderedDict()
        self.ttl = ttl

    def __setitem__(self, key, value):
        if key in self.data:
            del self.data[key]
        self.data[key] = (time.time(), value)
        self.cull()

    def cull(self):
        for k, v in self.iter_items_older_than(self.ttl):
            self.data.popitem(last=False)

    def get(self, key, default=None):
        self.cull()
        if key in self.data:
            return self[key]
        return default

    def __getitem__(self, key):
        self.cull()
        return self.data[key][1]

    def __iter__(self):
        self.cull()
        return iter(self.data)

    def __repr__(self):
        self.cull()
        return repr(self.data)

    def iter_items_older_than(self, seconds_old):
        min_birthday = time.time() - seconds_old
        zipped = self._triple_iterable()
        matches = takewhile(lambda r: min_birthday >= r[1], zipped)
        return list(map(operator.itemgetter(0, 2), matches))

    def _triple_iterable(self):
        ikeys = self.data.keys()
        ibirthday = map(operator.itemgetter(0), self.data.values())
        ivalues = map(operator.itemgetter(1), self.data.values())
        return zip(ikeys, ibirthday, ivalues)

    def items(self):
        self.cull()
        ikeys = self.data.keys()
        ivalues = map(operator.itemgetter(1), self.data.values())
        return zip(ikeys, ivalues)
