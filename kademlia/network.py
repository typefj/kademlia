"""
Package for interacting on the network at a high level.
"""
import random
import pickle
import asyncio
from logging import getLogger

from .protocol import KademliaProtocol
from .utils import digest
from .storage import ForgetfulStorage
from .node import Node
from .crawling import ValueSpiderCrawl
from .crawling import NodeSpiderCrawl


class Server(object):
    """
    High level view of a node instance.  This is the object that should be created
    to start listening as an active node on the network.
    """

    def __init__(self, k_size=20, alpha=3, nid=None, storage=None):
        """
        Create a server instance.  This will start listening on the given port.

        Args:
            k_size (int): The k parameter from the paper
            alpha (int): The alpha parameter from the paper
            nid: The id for this node on the network.
            storage: An instance that implements :interface:`~kademlia.storage.IStorage`
        """
        self.k_size = k_size
        self.alpha = alpha
        self.log = getLogger("kademlia-server")
        self.storage = storage or ForgetfulStorage()
        self.node = Node(nid or digest(random.getrandbits(255)))
        self.transport = None
        self.protocol = None
        self.refresh_loop = None

    def stop(self):
        if self.refresh_loop is not None:
            self.refresh_loop.cancel()

        if self.transport is not None:
            self.transport.close()

    def listen(self, port, interface='0.0.0.0'):
        """
        Start listening on the given port.

        Provide interface="::" to accept ipv6 address
        """
        proto_factory = lambda: KademliaProtocol(self.node, self.storage, self.k_size)
        loop = asyncio.get_event_loop()
        listen = loop.create_datagram_endpoint(proto_factory, local_addr=(interface, port))
        self.transport, self.protocol = loop.run_until_complete(listen)
        # finally, schedule refreshing table
        self.refresh_table()

    def refresh_table(self):
        asyncio.ensure_future(self._refresh_table())
        loop = asyncio.get_event_loop()
        self.refresh_loop = loop.call_later(3600, self.refresh_table)

    async def _refresh_table(self):
        """
        Refresh buckets that haven't had any lookups in the last hour
        (per section 2.3 of the paper).
        """
        ds = []
        for nid in self.protocol.iter_refresh_ids():
            node = Node(nid)
            nearest = self.protocol.router.find_neighbors(node, self.alpha)
            spider = NodeSpiderCrawl(self.protocol, node, nearest, self.k_size, self.alpha)
            ds.append(spider.find())

        # do our crawling
        await asyncio.gather(*ds)

        # now republish keys older than one hour
        for dkey, value in self.storage.iter_items_older_than(3600):
            await self.set_digest(dkey, value)

    def bootstrap_neighbors(self):
        """
        Get a :class:`list` of (ip, port) :class:`tuple` pairs suitable for use as an argument
        to the bootstrap method.

        The server should have been bootstrapped
        already - this is just a utility for getting some neighbors and then
        storing them if this server is going down for a while.  When it comes
        back up, the list of nodes can be used to bootstrap.
        """
        neighbors = self.protocol.router.find_neighbors(self.node)
        return [tuple(n)[-2:] for n in neighbors]

    async def bootstrap(self, addrs):
        """
        Bootstrap the server by connecting to other known nodes in the network.

        Args:
            addrs: A `list` of (ip, port) `tuple` pairs.  Note that only IP addresses
                   are acceptable - hostnames will cause an error.
        """
        cos = list(map(self.bootstrap_node, addrs))
        nodes = [node for node in await asyncio.gather(*cos) if node is not None]
        spider = NodeSpiderCrawl(self.protocol, self.node, nodes, self.k_size, self.alpha)
        return await spider.find()

    async def bootstrap_node(self, addr):
        result = await self.protocol.ping(addr, self.node.nid)
        return Node(result[1], addr[0], addr[1]) if result[0] else None

    def inet_visible_ip(self):
        """
        Get the internet visible IP's of this node as other nodes see it.

        Returns:
            A `list` of IP's.  If no one can be contacted, then the `list` will be empty.
        """

        def handle(results):
            ips = [result[1][0] for result in results if result[0]]
            self.log.debug("other nodes think our ip is %s" % str(ips))
            return ips

        ds = []
        for neighbor in self.bootstrap_neighbors():
            ds.append(self.protocol.stun(neighbor))
        # return defer.gatherResults(ds).addCallback(handle)

    async def get(self, key):
        """
        Get a key if the network has it.

        Returns:
            :class:`None` if not found, the value otherwise.
        """
        dkey = digest(key)
        # if this node has it, return it
        if self.storage.get(dkey) is not None:
            return self.storage.get(dkey)
        node = Node(dkey)
        nearest = self.protocol.router.find_neighbors(node)
        if len(nearest) == 0:
            self.log.warning("There are no known neighbors to get key %s" % key)
            return None
        spider = ValueSpiderCrawl(self.protocol, node, nearest, self.k_size, self.alpha)
        return await spider.find()

    async def set(self, key, value):
        """
        Set the given string key to the given value in the network.
        """
        self.log.debug("setting '%s' = '%s' on network" % (key, value))
        dkey = digest(key)
        return await self.set_digest(dkey, value)

    async def set_digest(self, dkey, value):
        """
        Set the given SHA1 digest key (bytes) to the given value in the network.
        """
        node = Node(dkey)

        nearest = self.protocol.router.find_neighbors(node)
        if len(nearest) == 0:
            self.log.warning("There are no known neighbors to set key %s" % dkey.hex())
            return False

        spider = NodeSpiderCrawl(self.protocol, node, nearest, self.k_size, self.alpha)
        nodes = await spider.find()
        self.log.info("setting '%s' on %s" % (dkey.hex(), list(map(str, nodes))))

        # if this node is close too, then store here as well
        if self.node.distance(node) < max([n.distance(node) for n in nodes]):
            self.storage[dkey] = value
        ds = [self.protocol.call_store(n, dkey, value) for n in nodes]
        # return true only if at least one store call succeeded
        return any(await asyncio.gather(*ds))

    def save_state(self, fname):
        """
        Save the state of this node (the alpha/k_size/id/immediate neighbors)
        to a cache file with the given fname.
        """
        data = {'k_size': self.k_size,
                'alpha': self.alpha,
                'id': self.node.nid,
                'neighbors': self.bootstrap_neighbors()}
        if len(data['neighbors']) == 0:
            self.log.warning("No known neighbors, so not writing to cache.")
            return
        with open(fname, 'w') as f:
            pickle.dump(data, f)

    @classmethod
    def load_state(self, fname):
        """
        Load the state of this node (the alpha/k_size/id/immediate neighbors)
        from a cache file with the given fname.
        """
        with open(fname, 'r') as f:
            data = pickle.load(f)
        s = Server(data['k_size'], data['alpha'], data['id'])
        if len(data['neighbors']) > 0:
            s.bootstrap(data['neighbors'])
        return s

    def save_state_regularly(self, fname, frequency=600):
        """
        Save the state of node with a given regularity to the given
        filename.

        Args:
            fname: File name to save retularly to
            frequencey: Frequency in seconds that the state should be saved.
                        By default, 10 minutes.
        """

        # loop = LoopingCall(self.saveState, fname)
        # loop.start(frequency)
        # return loop
