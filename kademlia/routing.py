import heapq
import time
import operator
import asyncio
import os
from collections import OrderedDict
from .utils import bytes2bitstring


class KBucket(object):
    def __init__(self, min_range, max_range, k_size):
        self.min_range = min_range
        self.max_range = max_range
        self.range = (min_range, max_range)
        self.k_size = k_size
        self.last_updated = time.time()
        self.nodes = OrderedDict()
        self.replacement_nodes = []

    def get_nodes(self):
        return list(self.nodes.values())

    def split(self):
        midpoint = (self.min_range + self.max_range) / 2
        bucket1 = KBucket(self.min_range, midpoint, self.k_size)
        bucket2 = KBucket(midpoint + 1, self.max_range, self.k_size)
        for node in self.nodes.values():
            bucket = bucket1 if node.long_id <= midpoint else bucket2
            bucket.nodes[node.nid] = node
        return bucket1, bucket2

    def touch_bucket(self):
        self.last_updated = time.time()

    def remove_node(self, node):
        if node.nid not in self.nodes:
            return
        del self.nodes[node.nid]
        if self.replacement_nodes:
            new_node = self.replacement_nodes.pop()
            self.nodes[new_node.nid] = new_node

    def is_new_node(self, node):
        return node.nid not in self.nodes

    def is_in_range(self, node):
        return self.min_range <= node.long_id <= self.max_range

    def add_node(self, node):
        if node.nid in self.nodes:
            del self.nodes[node.nid]
            self.nodes[node.nid] = node
        elif len(self) < self.k_size:
            self.nodes[node.nid] = node
        else:
            if node in self.replacement_nodes:
                self.replacement_nodes.remove(node)
            self.replacement_nodes.append(node)
            return False
        return True

    def head(self):
        return self.get_nodes()[0]

    def depth(self):
        return len(os.path.commonprefix([bytes2bitstring(n.nid) for n in self.nodes.values()]))

    def __len__(self):
        return len(self.nodes)

    def __getitem__(self, item):
        return self.nodes.get(item, None)


class TableTraverser(object):
    def __init__(self, table, start_node):
        index = table.get_bucket_index(start_node)
        table.buckets[index].touch_bucket()
        self.current_nodes = table.buckets[index].get_nodes()
        self.left_buckets = table.buckets[:index]
        self.right_buckets = table.buckets[(index + 1):]
        self.left = True

    def __iter__(self):
        return self

    def __next__(self):
        """
        Pop an item from the left subtree, then right, then left, etc.
        """
        if self.current_nodes:
            return self.current_nodes.pop()

        if self.left and len(self.left_buckets) > 0:
            self.current_nodes = self.left_buckets.pop().get_nodes()
            self.left = False
            return next(self)

        if len(self.right_buckets) > 0:
            self.currentNodes = self.right_buckets.pop().get_nodes()
            self.left = True
            return next(self)

        raise StopIteration


class RoutingTable(object):
    def __init__(self, protocol, k_size, node):
        self.node = node
        self.protocol = protocol
        self.k_size = k_size
        self.buckets = []
        self.flush()

    def flush(self):
        self.buckets = [KBucket(0, 2 ** 160, self.k_size)]

    def split_bucket(self, index):
        one, two = self.buckets[index].split()
        self.buckets[index] = one
        self.buckets.insert(index + 1, two)

    def get_lonely_buckets(self):
        return [b for b in self.buckets if b.last_updated < (time.time() - 3600)]

    def remove_contact(self, node):
        index = self.get_bucket_index(node)
        self.buckets[index].remove_node(node)

    def is_new_node(self, node):
        index = self.get_bucket_index(node)
        return self.buckets[index].is_new_node(node)

    def add_contact(self, node):
        index = self.get_bucket_index(node)
        bucket = self.buckets[index]

        # this will succeed unless the bucket is full
        if bucket.add_node(node):
            return

        # Per section 4.2 of paper, split if the bucket has the node in its range
        # or if the depth is not congruent to 0 mod 5
        if bucket.is_in_range(self.node) or bucket.depth() % 5 != 0:
            self.split_bucket(index)
            self.add_contact(node)
        else:
            asyncio.ensure_future(self.protocol.callPing(bucket.head()))

    def get_bucket_index(self, node):
        for index, bucket in enumerate(self.buckets):
            if node.long_id < bucket.range[1]:
                return index

    def find_neighbors(self, node, k=None, exclude=None):
        k = k or self.k_size
        nodes = []
        for neighbor in TableTraverser(self, node):
            if neighbor.nid != node.nid and (exclude is None or not neighbor.same_home_as(exclude)):
                heapq.heappush(nodes, (node.distance(neighbor), neighbor))
            if len(nodes) == k:
                break
        return list(map(operator.itemgetter(1), heapq.nsmallest(k, nodes)))

