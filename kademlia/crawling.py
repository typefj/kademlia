from collections import Counter
from logging import getLogger

from .node import Node, NodeHeap
from .utils import gather_dict


class SpiderCrawl(object):
    """
    Crawl the network and look for given 160-bit keys.
    """

    def __init__(self, protocol, node, peers, k_size, alpha):
        """
        Create a new C{SpiderCrawl}er.

        Args:
            protocol: A :class:`~kademlia.protocol.KademliaProtocol` instance.
            node: A :class:`~kademlia.node.Node` representing the key we're looking for
            peers: A list of :class:`~kademlia.node.Node` instances that provide the entry point for the network
            k_size: The value for k based on the paper
            alpha: The value for alpha based on the paper
        """
        self.protocol = protocol
        self.k_size = k_size
        self.alpha = alpha
        self.node = node
        self.nearest = NodeHeap(self.node, self.k_size)
        self.last_ids_crawled = []
        self.log = getLogger("kademlia-spider")
        self.log.info("creating spider with peers: %s" % peers)
        self.nearest.push(peers)

    async def _find(self, rpcmethod):
        """
        Get either a value or list of nodes.

        Args:
            rpcmethod: The protocol's callfindValue or callFindNode.

        The process:
          1. calls find_* to current ALPHA nearest not already queried nodes,
             adding results to current nearest list of k nodes.
          2. current nearest list needs to keep track of who has been queried already
             sort by nearest, keep k_size
          3. if list is same as last time, next call should be to everyone not
             yet queried
          4. repeat, unless nearest list has all been queried, then ur done
        """
        self.log.info("crawling with nearest: %s" % str(tuple(self.nearest)))
        count = self.alpha
        if self.nearest.get_nids() == self.last_ids_crawled:
            self.log.info("last iteration same as current - checking all in list now")
            count = len(self.nearest)
        self.last_ids_crawled = self.nearest.get_nids()

        ds = {}
        for peer in self.nearest.get_uncontacted()[:count]:
            ds[peer.id] = rpcmethod(peer, self.node)
            self.nearest.mark_contacted(peer)
        found = await gather_dict(ds)
        return await self._nodes_found(found)


class ValueSpiderCrawl(SpiderCrawl):
    def __init__(self, protocol, node, peers, k_size, alpha):
        SpiderCrawl.__init__(self, protocol, node, peers, k_size, alpha)
        # keep track of the single nearest node without value - per
        # section 2.3 so we can set the key there if found
        self.nearest_without_value = NodeHeap(self.node, 1)

    async def find(self):
        """
        Find either the closest nodes or the value requested.
        """
        return await self._find(self.protocol.call_find_value)

    async def _nodes_found(self, responses):
        """
        Handle the result of an iteration in _find.
        """
        to_remove = []
        found_values = []
        for peerid, response in responses.items():
            response = RPCFindResponse(response)
            if not response.happened():
                to_remove.append(peerid)
            elif response.has_value():
                found_values.append(response.get_value())
            else:
                peer = self.nearest.get_node_by_nid(peerid)
                self.nearest_without_value.push(peer)
                self.nearest.push(response.get_node_list())
        self.nearest.remove(to_remove)

        if len(found_values) > 0:
            return await self._handle_found_values(found_values)
        if self.nearest.all_been_contacted():
            # not found!
            return None
        return await self.find()

    async def _handle_found_values(self, values):
        """
        We got some values!  Exciting.  But let's make sure
        they're all the same or freak out a little bit.  Also,
        make sure we tell the nearest node that *didn't* have
        the value to store it.
        """
        value_counts = Counter(values)
        if len(value_counts) != 1:
            args = (self.node.long_id, str(values))
            self.log.warning("Got multiple values for key %i: %s" % args)
        value = value_counts.most_common(1)[0][0]

        peer_to_save_to = self.nearest_without_value.popleft()
        if peer_to_save_to is not None:
            await self.protocol.call_store(peer_to_save_to, self.node.id, value)
        return value


class NodeSpiderCrawl(SpiderCrawl):
    async def find(self):
        """
        Find the closest nodes.
        """
        return await self._find(self.protocol.call_find_node)

    async def _nodes_found(self, responses):
        """
        Handle the result of an iteration in _find.
        """
        to_remove = []
        for peerid, response in responses.items():
            response = RPCFindResponse(response)
            if not response.happened():
                to_remove.append(peerid)
            else:
                self.nearest.push(response.get_node_list())
        self.nearest.remove(to_remove)

        if self.nearest.all_been_contacted():
            return list(self.nearest)
        return self.find()


class RPCFindResponse(object):
    def __init__(self, response):
        """
        A wrapper for the result of a RPC find.

        Args:
            response: This will be a tuple of (<response received>, <value>)
                      where <value> will be a list of tuples if not found or
                      a dictionary of {'value': v} where v is the value desired
        """
        self.response = response

    def happened(self):
        """
        Did the other host actually respond?
        """
        return self.response[0]

    def has_value(self):
        return isinstance(self.response[1], dict)

    def get_value(self):
        return self.response[1]['value']

    def get_node_list(self):
        """
        Get the node list in the response.  If there's no value, this should
        be set.
        """
        nodelist = self.response[1] or []
        return [Node(*nodeple) for nodeple in nodelist]
