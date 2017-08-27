"""
General catchall for functions that don't make sense as methods.
"""
import hashlib
import asyncio
from _socket import inet_ntoa, inet_aton
from functools import reduce
from struct import unpack, pack


async def gather_dict(d):
    cors = list(d.values())
    results = await asyncio.gather(*cors)
    return dict(zip(d.keys(), results))


def digest(s):
    if not isinstance(s, bytes):
        s = str(s).encode('utf8')
    return hashlib.sha1(s).digest()


def bytes2bitstring(_bytes):
    return "".join([bin(byte)[2:].rjust(8, '0') for byte in _bytes])


def decode_nodes(nodes):
    nodes = nodes if isinstance(nodes, bytes) else nodes.encode('latin1')
    n = {}
    length = len(nodes)
    if (length % 26) != 0:
        return n.values()

    for i in range(0, length, 26):
        nid = nodes[i:i + 20]
        ip = inet_ntoa(nodes[i + 20:i + 24])
        port = unpack('!H', nodes[i + 24:i + 26])[0]
        n[nid] = (nid, ip, port)
    return n.values()


def encode_nodes(node_list):
    return reduce(lambda x, y: x + y, map(lambda x: x[0] + inet_aton(x[1]) + pack('!H', x[2]), node_list)).decode('latin1')
