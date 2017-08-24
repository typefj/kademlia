"""
General catchall for functions that don't make sense as methods.
"""
import hashlib
import asyncio


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
