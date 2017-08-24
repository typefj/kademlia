import random
import asyncio
from logging import getLogger

from rpcudp.protocol import RPCProtocol

from .node import Node
from .routing import RoutingTable
from .utils import digest


class KademliaProtocol(RPCProtocol):
    def __init__(self, source_node, storage, k_size):
        RPCProtocol.__init__(self)
        self.router = RoutingTable(self, k_size, source_node)
        self.storage = storage
        self.source_node = source_node
        self.log = getLogger("kademlia-protocol")

    def iter_refresh_ids(self):
        for bucket in self.router.get_lonely_buckets():
            yield random.randint(*bucket.range).to_bytes(20, byteorder='big')

    def rpc_stun(self, sender):
        return sender

    def rpc_ping(self, sender, nodeid):
        source = Node(nodeid, sender[0], sender[1])
        self.welcome_new_node(source)
        return self.source_node.nid

    def rpc_store(self, sender, nodeid, key, value):
        source = Node(nodeid, sender[0], sender[1])
        self.welcome_new_node(source)
        self.log.debug("got a store request from %s, storing value" % str(sender))
        self.storage[key] = value
        return True

    def rpc_find_node(self, sender, nodeid, key):
        self.log.info("finding neighbors of %i in local table" % int(nodeid.hex(), 16))
        source = Node(nodeid, sender[0], sender[1])
        self.welcome_new_node(source)
        node = Node(key)
        return list(map(tuple, self.router.find_neighbors(node, exclude=source)))

    def rpc_find_value(self, sender, nodeid, key):
        source = Node(nodeid, sender[0], sender[1])
        self.welcome_new_node(source)
        value = self.storage.get(key, None)
        if value is None:
            return self.rpc_find_node(sender, nodeid, key)
        return {'value': value}

    async def call_find_node(self, node_to_ask, node_to_find):
        address = (node_to_ask.ip, node_to_ask.port)
        result = await self.find_node(address, self.source_node.nid, node_to_find.nid)
        return self.handle_call_response(result, node_to_ask)

    async def call_find_value(self, node_to_ask, node_to_find):
        address = (node_to_ask.ip, node_to_ask.port)
        result = await self.find_value(address, self.source_node.nid, node_to_find.nid)
        return self.handle_call_response(result, node_to_ask)

    async def call_ping(self, node_to_ask):
        address = (node_to_ask.ip, node_to_ask.port)
        result = await self.ping(address, self.source_node.nid)
        return self.handle_call_response(result, node_to_ask)

    async def call_store(self, node_to_ask, key, value):
        address = (node_to_ask.ip, node_to_ask.port)
        result = await self.store(address, self.source_node.nid, key, value)
        return self.handle_call_response(result, node_to_ask)

    def welcome_new_node(self, node):
        """
        Given a new node, send it all the keys/values it should be storing,
        then add it to the routing table.

        @param node: A new node that just joined (or that we just found out
        about).

        Process:
        For each key in storage, get k closest nodes.  If newnode is closer
        than the furtherst in that list, and the node for this server
        is closer than the closest in that list, then store the key/value
        on the new node (per section 2.5 of the paper)
        """
        if not self.router.is_new_node(node):
            return

        self.log.info("never seen %s before, adding to router and setting nearby " % node)
        for key, value in self.storage.items():
            keynode = Node(digest(key))
            neighbors = self.router.find_neighbors(keynode)
            s = False
            if len(neighbors) > 0:
                new_node_close = node.distance(keynode) < neighbors[-1].distance(keynode)
                this_node_closest = self.source_node.distance(keynode) < neighbors[0].distance(keynode)
                s = (new_node_close and this_node_closest)
            if len(neighbors) == 0 or s:
                asyncio.ensure_future(self.call_store(node, key, value))
        self.router.add_contact(node)

    def handle_call_response(self, result, node):
        """
        If we get a response, add the node to the routing table.  If
        we get no response, make sure it's removed from the routing table.
        """
        if not result[0]:
            self.log.warning("no response from %s, removing from router" % node)
            self.router.remove_contact(node)
            return result

        self.log.info("got successful response from %s")
        self.welcome_new_node(node)
        return result
