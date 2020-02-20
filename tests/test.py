import asyncio
import unittest
import functools
from justredis import Multiplexer, RedisError, RedisReplyError, utf8_bytes_as_strings
from .redis_server import RedisServer, start_cluster


def asynctest(asyncfunc):
    @functools.wraps(asyncfunc)
    def asyncwrapper(*args, **kwargs):
        return asyncio.get_event_loop().run_until_complete(asyncfunc(*args, **kwargs))
    return asyncwrapper


class TestServerWithPassword(unittest.TestCase):
    def setUp(self):
        self.server = RedisServer(extraparams='--requirepass blah')
        self.mp = Multiplexer({'endpoints': ('localhost', self.server.port), 'password': 'blah'})
        self.c0 = self.mp.database(0).command
        self.cr0 = self.mp.database(0).commandreply
    
    @asynctest
    async def tearDown(self):
        self.cr0 = None
        self.c0 = None
        await self.mp.close()
        self.mp = None
        self.server.close()
        self.server = None

    @asynctest
    async def test_wrongpassword(self):
        mp = Multiplexer({'endpoints': ('localhost', self.server.port), 'password': 'wrong'})
        with self.assertRaises(RedisReplyError):
            await mp.database(0).commandreply(b'GET', b'a')
        mp = Multiplexer({'endpoints': ('localhost', self.server.port)})
        with self.assertRaises(RedisReplyError):
            await mp.database(0).commandreply(b'GET', b'a')
        self.assertEqual(await self.cr0(b'GET', b'a'), None)
