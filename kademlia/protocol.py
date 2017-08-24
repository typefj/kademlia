import asyncio
import os
import random
from base64 import b64encode
from hashlib import sha1
from logging import getLogger

import umsgpack

from .node import Node
from .routing import RoutingTable
from .utils import digest

log = getLogger('rpcudp')


class MalformedMessage(Exception):
    """
    Message does not contain what is expected.
    """


class RPCProtocol(asyncio.DatagramProtocol):
    def __init__(self, wait_timeout=10):
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
        asyncio.ensure_future(self._solve_datagram(datagram, address))

    @asyncio.coroutine
    def _solve_datagram(self, datagram, address):
        if len(datagram) < 22:
            log.warning("received datagram too small from %s, ignoring", address)
            return

        msg_id = datagram[1:21]
        data = umsgpack.unpackb(datagram[21:])

        if datagram[:1] == b'\x00':
            # schedule accepting request and returning the result
            asyncio.ensure_future(self._accept_request(msg_id, data, address))
        elif datagram[:1] == b'\x01':
            self._accept_response(msg_id, data, address)
        else:
            # otherwise, don't know the format, don't do anything
            log.debug("Received unknown message from %s, ignoring", address)

    def _accept_response(self, msg_id, data, address):
        msgargs = (b64encode(msg_id), address)
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
        if not isinstance(data, list) or len(data) != 2:
            raise MalformedMessage("Could not read packet: %s" % data)
        funcname, args = data
        f = getattr(self, "rpc_%s" % funcname, None)
        if f is None or not callable(f):
            msgargs = (self.__class__.__name__, funcname)
            log.warning("%s has no callable method rpc_%s; ignoring request", *msgargs)
            return

        if not asyncio.iscoroutinefunction(f):
            f = asyncio.coroutine(f)
        response = yield from f(address, *args)
        log.debug("sending response %s for msg id %s to %s", response, b64encode(msg_id), address)
        txdata = b'\x01' + msg_id + umsgpack.packb(response)
        self.transport.sendto(txdata, address)

    def _timeout(self, msg_id):
        args = (b64encode(msg_id), self._wait_timeout)
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

        def func(address, *args):
            msg_id = sha1(os.urandom(32)).digest()
            data = umsgpack.packb([name, args])
            if len(data) > 8192:
                msg = "Total length of function name and arguments cannot exceed 8K"
                raise MalformedMessage(msg)
            txdata = b'\x00' + msg_id + data
            log.debug("calling remote function %s on %s (msgid %s)", name, address, b64encode(msg_id))
            self.transport.sendto(txdata, address)

            loop = asyncio.get_event_loop()
            f = loop.create_future() if hasattr(loop, 'create_future') else asyncio.Future()
            timeout = loop.call_later(self._wait_timeout, self._timeout, msg_id)
            self._outstanding[msg_id] = (f, timeout)
            return f

        return func


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
