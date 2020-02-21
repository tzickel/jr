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

    @asynctest
    async def test_simple(self):
        self.assertEqual(await (await self.c0(b'SET', b'a', b'b'))(), b'OK')
        self.assertEqual(await (await self.c0(b'GET', b'a'))(), b'b')
        self.assertEqual(await self.cr0(b'GET', b'a'), b'b')

    @asynctest
    async def test_notallowed(self):
        with self.assertRaises(RedisError):
            await self.cr0(b'AUTH', b'asd')

    @asynctest
    async def test_some_encodings(self):
        with self.assertRaises(ValueError):
            await self.cr0(b'SET', 'a', True)
        self.assertEqual(await self.cr0(b'INCRBYFLOAT', 'float_check', 0.1), b'0.1')
        with self.assertRaises(ValueError):
            await self.cr0(b'SET', 'a', [1, 2])
        await self.cr0(b'SET', 'check_a', 'a')
        await self.cr0(b'SET', 'check_b', 'b')
        self.assertEqual(await self.cr0(b'GET', 'check_a', decoder=utf8_bytes_as_strings), 'a')
        self.assertEqual(await self.cr0(b'MGET', 'check_a', 'check_b', decoder=utf8_bytes_as_strings), ['a', 'b'])

    @asynctest
    async def test_chunk_encoded_command(self):
        self.assertEqual(await self.cr0(b'SET', b'test_chunk_encoded_command_a', b'test_chunk_encoded_command_a'*10*1024), b'OK')
        self.assertEqual(await self.cr0(b'GET', b'test_chunk_encoded_command_a'), b'test_chunk_encoded_command_a'*10*1024)
        self.assertEqual(await self.cr0(b'MGET', b'test_chunk_encoded_command_a'* 3500, b'test_chunk_encoded_command_a'* 3500, b'test_chunk_encoded_command_a'* 3500), [None, None, None])

    @asynctest
    async def test_eval(self):
        self.assertEqual(await self.cr0('set', 'evaltest', b'a'), b'OK')
        self.assertEqual(await self.cr0('eval', "return redis.call('get','evaltest')", 0), b'a')
        self.assertEqual(await self.cr0('eval', "return redis.call('get','evaltestno')", 0), None)
        self.assertEqual(await self.cr0('eval', "return redis.call('get','evaltest')", 0), b'a')
        self.assertEqual(await self.cr0('eval', "return redis.call('get','evaltestno')", 0), None)
        self.assertEqual(await self.cr0('script', 'flush'), b'OK')
        self.assertEqual(await self.cr0('eval', "return redis.call('get','evaltest')", 0), b'a')
        self.assertEqual(await self.cr0('eval', "return redis.call('get','evaltestno')", 0), None)


class TestCluster(unittest.TestCase):
    @asynctest
    async def setUp(self):
        self.servers = start_cluster(3)
        self.mp = Multiplexer({'endpoints': ('127.0.0.1', self.servers[0].port)})
        self.c0 = self.mp.database().command
        self.cr0 = self.mp.database().commandreply
        # Sometimes it takes awhile for the cluster to be ready
        wait = 50
        while wait:
            result = await self.mp.run_commandreply_on_all_masters(b'CLUSTER', b'INFO')
            ready = True
            for res in result.values():
                if b'cluster_state:ok' not in res:
                    ready = False
            if ready:
                break
#            if b'cluster_state:ok' in self.cr0(b'CLUSTER', b'INFO'):
#                break
            wait -= 1
        if not wait:
            raise Exception('Cluster is down, could not run test')

    @asynctest
    async def tearDown(self):
        self.cr0 = None
        self.c0 = None
        await self.mp.close()
        self.mp = None
        # TODO close as well?
        self.servers = None

    @asynctest
    async def test_basic(self):
        self.assertEqual(await self.cr0(b'set', b'a', b'a'), b'OK')
        self.assertEqual(await self.cr0(b'set', b'b', b'b'), b'OK')
        self.assertEqual(await self.cr0(b'set', b'c', b'c'), b'OK')
        self.assertEqual(await self.cr0(b'set', b'{a}b', b'd'), b'OK')
        with self.assertRaises(RedisError):
            await self.mp.database(1).command(b'set', b'a', 'b')

    @asynctest
    async def test_outoforder(self):
        c1 = await self.c0(b'set', b'a', b'a')
        c2 = await self.c0(b'set', b'b', b'b')
        c3 = await self.c0(b'set', b'c', b'c')
        c4 = await self.c0(b'set', b'{a}b', b'ab')
        with self.assertRaises(RedisError):
            await self.mp.database(1).command(b'set', b'a', 'b')
        self.assertEqual(await c1(), b'OK')
        self.assertEqual(await c2(), b'OK')
        self.assertEqual(await c3(), b'OK')
        self.assertEqual(await c4(), b'OK')

    @asynctest
    async def test_misc(self):
        # This tests an command which redis server says keys start in index 2.
        await self.cr0(b'object', b'help')
        # Check command with no keys
        await self.cr0(b'client', b'list')

    @asynctest
    async def test_server(self):
        self.assertEqual(await self.cr0(b'set', b'aa', b'a'), b'OK')
        self.assertEqual(await self.cr0(b'set', b'bb', b'b'), b'OK')
        self.assertEqual(await self.cr0(b'set', b'cc', b'c'), b'OK')
        result = await self.mp.run_commandreply_on_all_masters(b'KEYS', b'*')
        self.assertEqual(len(result), 3)
        result = list(result.values())
        result = [i for s in result for i in s]
        self.assertEqual(set(result), set([b'aa', b'bb', b'cc']))

    @asynctest
    async def test_moved(self):
        self.assertEqual(await self.cr0(b'set', b'aa', b'a'), b'OK')
        self.assertEqual(await self.cr0(b'set', b'bb', b'b'), b'OK')
        self.assertEqual(await self.cr0(b'set', b'cc', b'c'), b'OK')
        result = await self.mp.run_commandreply_on_all_masters(b'GET', b'aa')
        self.assertEqual(len(result), 3)
        result = list(result.values())
        self.assertEqual(result, [b'a', b'a', b'a'])
