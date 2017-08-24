from operator import itemgetter
import heapq


class Node(object):
    def __init__(self, nid, ip=None, port=None):
        self.nid = nid
        self.ip = ip
        self.port = port
        self.long_id = int(nid.hex(), 16)

    def same_home_as(self, node):
        return self.ip == node.ip and self.port == node.port

    def distance(self, node):
        return self.long_id ^ node.long_id

    def __iter__(self):
        return iter([self.nid, self.ip, self.port])

    def __repr__(self):
        return repr([self.long_id, self.ip, self.port])

    def __str__(self):
        return "%s:%s" % (self.ip, str(self.port))


class NodeHeap(object):
    def __init__(self, node, maxsize):
        self.node = node
        self.heap = []
        self.contacted = set()
        self.maxsize = maxsize

    def remove(self, peers):
        peers = set(peers)
        if len(peers) == 0:
            return
        nheap = []
        for distance, node in self.heap:
            if node.nid not in peers:
                heapq.heappush(nheap, (distance, node))
        self.heap = nheap

    def get_node_by_nid(self, nid):
        for _, node in self.heap:
            if node.nid == nid:
                return node
        return None

    def all_been_contacted(self):
        return len(self.get_uncontacted()) == 0

    def get_nids(self):
        return [n.nid for n in self]

    def mark_contacted(self, node):
        self.contacted.add(node.nid)

    def popleft(self):
        if len(self) > 0:
            return heapq.heappop(self.heap)[1]
        return None

    def push(self, nodes):
        if not isinstance(nodes, list):
            nodes = [nodes]
        for node in nodes:
            if node not in self:
                distance = self.node.distance(node)
                heapq.heappush(self.heap, (distance, node))

    def __len__(self):
        return min(len(self.heap), self.maxsize)

    def __iter__(self):
        nodes = heapq.nsmallest(self.maxsize, self.heap)
        return iter(map(itemgetter(1), nodes))

    def __contains__(self, node):
        for distance, n in self.heap:
            if node.nid == n.nid:
                return True
        return False

    def get_uncontacted(self):
        return [n for n in self if n.nid not in self.contacted]

