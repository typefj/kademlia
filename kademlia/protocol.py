import asyncio
import random
from logging import getLogger
import os
from bencode import bencode, bdecode, BTFailure

from .node import Node
from .routing import RoutingTable
from .utils import digest, encode_nodes

log = getLogger('rpcudp')


class RPCProtocol(asyncio.DatagramProtocol):
    def __init__(self, wait_timeout=5):
        """
        @param wait_timeout: Consider it a connetion failure if no response
        within this time window.
        """
        self._wait_timeout = wait_timeout
        self._outstanding = {}

    # noinspection PyAttributeOutsideInit
    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, datagram, address):
        log.debug("received datagram from %s", address)
        try:
            data = bdecode(datagram.decode('latin1'))
        except BTFailure as e:
            log.warning('decode data failure. {}'.format(e))
        else:
            asyncio.ensure_future(self._solve_datagram(data, address))

    @asyncio.coroutine
    def _solve_datagram(self, data, address):
        msg_id = data['t']
        y = data['y']
        if y == 'q':
            # schedule accepting request and returning the result
            asyncio.ensure_future(self._accept_request(msg_id, data, address))
        elif y == 'r':
            self._accept_response(msg_id, data, address)
        else:
            # otherwise, don't know the format, don't do anything
            log.debug("Received unknown message from {}, ignoring. data: {}".format(address, data))

    def _accept_response(self, msg_id, data, address):
        msgargs = (msg_id, address)
        if msg_id not in self._outstanding:
            log.warning("received unknown message %s from %s; ignoring", *msgargs)
            return
        log.debug("received response %s for message id %s from %s", data, *msgargs)
        f, timeout = self._outstanding[msg_id]
        timeout.cancel()
        f.set_result((True, data))
        del self._outstanding[msg_id]

    @asyncio.coroutine
    def _accept_request(self, msg_id, data, address):
        funcname = data['q']
        f = getattr(self, "rpc_%s" % funcname, None)
        if f is None or not callable(f):
            msgargs = (self.__class__.__name__, funcname)
            log.warning("%s has no callable method rpc_%s; ignoring request", *msgargs)
            return

        if not asyncio.iscoroutinefunction(f):
            f = asyncio.coroutine(f)
        response = yield from f(address, data)
        log.debug("sending response %s for msg id %s to %s", response, msg_id, address)
        self.transport.sendto(bencode(response).encode('latin1'), address)

    def _timeout(self, msg_id):
        args = (msg_id, self._wait_timeout)
        log.error("Did not received reply for msg id %s within %i seconds", *args)
        self._outstanding[msg_id][0].set_result((False, None))
        del self._outstanding[msg_id]

    # noinspection PyUnresolvedReferences
    def __getattr__(self, name):
        if name.startswith("_") or name.startswith("rpc_"):
            return object.__getattr__(self, name)

        try:
            return object.__getattr__(self, name)
        except AttributeError:
            pass


def kprc_q(func):
    def method(self, *args, **kwargs):
        address, msg = func(self, *args, **kwargs)
        msg_id = msg['t']

        log.debug("calling remote function %s on %s (msgid %s)", func.__name__, address, msg_id)
        self.transport.sendto(bencode(msg).encode('latin1'), address)
        loop = asyncio.get_event_loop()
        f = loop.create_future() if hasattr(loop, 'create_future') else asyncio.Future()
        timeout = loop.call_later(self._wait_timeout, self._timeout, msg_id)
        self._outstanding[msg_id] = (f, timeout)
        return f

    return method


class KademliaProtocol(RPCProtocol):
    def __init__(self, source_node, storage, k_size):
        RPCProtocol.__init__(self)
        self.router = RoutingTable(self, k_size, source_node)
        self.storage = storage
        self.source_node = source_node
        self.log = getLogger("kademlia-protocol")

    @property
    def tid(self):
        return os.urandom(2)

    @property
    def gen_token(self):
        return os.urandom(4)

    def iter_refresh_ids(self):
        for bucket in self.router.get_lonely_buckets():
            yield random.randint(*bucket.range).to_bytes(20, byteorder='big')

    def rpc_stun(self, sender):
        return sender

    def rpc_ping(self, sender, data):
        nodeid = data['a']['id']
        source = Node(nodeid, sender[0], sender[1])
        self.welcome_new_node(source)
        return {"t": data['t'], "y": "r", "q": "ping", "a": {"id": self.source_node.str_nid}}

    @kprc_q
    def ping(self, address, node):
        msg = {"t": self.tid.hex(), "y": "q", "q": "ping", "a": {"id": node.str_nid}}
        return address, msg

    def rpc_store(self, sender, nodeid, key, value):
        source = Node(nodeid, sender[0], sender[1])
        self.welcome_new_node(source)
        self.log.debug("got a store request from %s, storing value" % str(sender))
        self.storage[key] = value
        return True

    def rpc_find_node(self, sender, data):
        target = data['a']['target']
        source_id = data['a']['id']
        self.log.info("finding neighbors of {} in local table".format(target))
        source = Node(source_id, sender[0], sender[1])
        self.welcome_new_node(source)
        node = Node(target)
        node_list = list(map(tuple, self.router.find_neighbors(node, exclude=source)))
        msg = {'r': {'id': self.source_node.str_nid, 'nodes': encode_nodes(node_list)}, 't': data['t'], 'y': 'r'}
        return msg

    @kprc_q
    def find_node(self, address, target):
        msg = {
            't': self.tid.hex(),
            'y': 'q',
            'q': 'find_node',
            'a': {
                'id': self.source_node.str_nid,
                'target': target.str_nid
            }
        }
        return address, msg

    @kprc_q
    def get_peers(self, address, info_hash):
        msg = {"t": self.tid.hex(), "y": "q", "q": "get_peers",
               "a": {"id": self.source_node.str_nid, "info_hash": info_hash}}
        return address, msg

    def rpc_get_peers(self, sender, data):
        source = Node(data['a']['id'], sender[0], sender[1])
        self.welcome_new_node(source)
        # value = self.storage.get(key, None)
        # if value is None:
        #     return self.rpc_find_node(sender, nodeid, key)
        # return {'value': value}
        node = Node(data['a']['info_hash'])
        node_list = list(map(tuple, self.router.find_neighbors(node, exclude=source)))
        return {"t": data['t'], "y": "r",
                "r": {"id": self.source_node.str_nid, "token": self.gen_token.hex(), "nodes": encode_nodes(node_list)}}

    async def call_find_node(self, node_to_ask, node_to_find):
        address = (node_to_ask.ip, node_to_ask.port)
        result = await self.find_node(address, node_to_find)
        return self.handle_call_response(result, node_to_ask)

    async def call_get_peers(self, node_to_ask, node_to_find):
        address = (node_to_ask.ip, node_to_ask.port)
        result = await self.get_peers(address, node_to_find.str_nid)
        return self.handle_call_response(result, node_to_ask)

    async def call_ping(self, node_to_ask):
        address = (node_to_ask.ip, node_to_ask.port)
        result = await self.ping(address, self.source_node)
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
