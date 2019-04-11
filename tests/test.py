#TODO check exception error msgs (https://github.com/cloudant/python-cloudant/issues/80)
import unittest
from justredis import Multiplexer, RedisError, RedisReplyError, utf8_bytes_as_strings
from .redis_server import RedisServer, start_cluster


class TestServerWithPassword(unittest.TestCase):
    def setUp(self):
        self.server = RedisServer(extraparams='--requirepass blah')
        self.mp = Multiplexer({'endpoints': ('localhost', self.server.port), 'password': 'blah'})
        self.c0 = self.mp.database(0).command
        self.cr0 = self.mp.database(0).commandreply
    
    def tearDown(self):
        self.cr0 = None
        self.c0 = None
        self.mp = None
        self.server = None

    def test_wrongpassword(self):
        mp = Multiplexer({'endpoints': ('localhost', self.server.port), 'password': 'wrong'})
        with self.assertRaises(RedisReplyError):
            mp.database(0).command(b'GET', b'a')()
        mp = Multiplexer({'endpoints': ('localhost', self.server.port)})
        with self.assertRaises(RedisReplyError):
            mp.database(0).command(b'GET', b'a')()

    def test_simple(self):
        c = self.mp.database(0).command
        cr = self.mp.database(0).commandreply
        cmd = c(b'SET', b'a', b'b')
        self.assertEqual(cmd(), b'OK')
        cmd = c(b'GET', b'a')
        self.assertEqual(cmd(), b'b')
        reply = cr(b'GET', b'a')
        self.assertEqual(reply, b'b')

    def test_notallowed(self):
        with self.assertRaises(RedisError):
            self.cr0(b'AUTH', b'asd')

    def test_some_encodings(self):
        cr = self.mp.database(0).commandreply
        with self.assertRaises(ValueError):
            cr(b'SET', 'a', True)
        self.assertEqual(cr(b'INCRBYFLOAT', 'float_check', 0.1), b'0.1')
        with self.assertRaises(ValueError):
            cr(b'SET', 'a', [1, 2])
        cr(b'SET', 'check_a', 'a')
        cr(b'SET', 'check_b', 'b')
        self.assertEqual(cr(b'GET', 'check_a', decoder=utf8_bytes_as_strings), 'a')
        self.assertEqual(cr(b'MGET', 'check_a', 'check_b', decoder=utf8_bytes_as_strings), ['a', 'b'])

    def test_chunk_encoded_command(self):
        self.assertEqual(self.cr0(b'SET', b'test_chunk_encoded_command_a', b'test_chunk_encoded_command_a'*10*1024), b'OK')
        self.assertEqual(self.cr0(b'GET', b'test_chunk_encoded_command_a'), b'test_chunk_encoded_command_a'*10*1024)
        self.assertEqual(self.cr0(b'MGET', b'test_chunk_encoded_command_a'* 3500, b'test_chunk_encoded_command_a'* 3500, b'test_chunk_encoded_command_a'* 3500), [None, None, None])

    def test_multi(self):
        with self.mp.database(0).multi() as m:
            cmd1 = m.command(b'SET', b'a', b'b')
            cmd2 = m.command(b'GET', b'a')
        self.assertEqual(cmd1(), b'OK')
        self.assertEqual(cmd2(), b'b')

        with self.mp.database(2).multi() as m0:
            cmd01 = m0.command(b'SET', b'a', b'b')
            with self.mp.database(3).multi() as m1:
                cmd11 = m1.command(b'SET', b'a1', b'c')
                cmd12 = m1.command(b'GET', b'a')
            cmd02 = m0.command(b'MGET', b'a', b'a1')
        self.assertEqual(cmd01(), b'OK')
        self.assertEqual(cmd02(), [b'b', None])
        self.assertEqual(cmd11(), b'OK')
        self.assertEqual(cmd12(), None)
        self.assertEqual(self.mp.database(2).command(b'GET', b'a')(), b'b')

    def test_multidiscard(self):
        with self.mp.database(0).multi() as m:
            cmd = m.command('nothing')
            m.discard()
        with self.assertRaises(RedisError):
            cmd()

        with self.mp.database(0).multi() as m:
            cmd = m.command('nothing')
            with self.assertRaises(RedisError):
                cmd()
            m.discard()
        with self.assertRaises(RedisError):
            cmd()

        with self.assertRaises(NameError):
            with self.mp.database(0).multi() as m:
                cmd = m.command('nothing')
                asd
                m.discard()
        with self.assertRaises(RedisError):
            cmd()

    def test_multierror(self):
        with self.mp.database(0).multi() as m:
            cmd = m.command('nosuchcommand')
        with self.assertRaises(RedisReplyError):
            cmd()

    def test_eval(self):
        self.assertEqual(self.cr0('set', 'evaltest', b'a'), b'OK')
        self.assertEqual(self.cr0('eval', "return redis.call('get','evaltest')", 0), b'a')
        self.assertEqual(self.cr0('eval', "return redis.call('get','evaltestno')", 0), None)
        self.assertEqual(self.cr0('eval', "return redis.call('get','evaltest')", 0), b'a')
        self.assertEqual(self.cr0('eval', "return redis.call('get','evaltestno')", 0), None)
        self.assertEqual(self.cr0('script', 'flush'), b'OK')
        self.assertEqual(self.cr0('eval', "return redis.call('get','evaltest')", 0), b'a')
        self.assertEqual(self.cr0('eval', "return redis.call('get','evaltestno')", 0), None)


class TestCluster(unittest.TestCase):
    def setUp(self):
        self.servers = start_cluster(3)
        self.mp = Multiplexer({'endpoints': ('127.0.0.1', self.servers[0].port)})
        self.c0 = self.mp.database().command
        self.cr0 = self.mp.database().commandreply
        # Sometimes it takes awhile for the cluster to be ready
        wait = 100
        while wait:
            result = self.mp.run_commandreply_on_all_masters(b'CLUSTER', b'INFO')
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
            raise Exception('Cluster is down, could not run test: %s' % result)

    def tearDown(self):
        self.cr0 = None
        self.c0 = None
        self.mp = None
        self.servers = None

    def test_basic(self):
        self.assertEqual(self.cr0(b'set', b'a', b'a'), b'OK')
        self.assertEqual(self.cr0(b'set', b'b', b'b'), b'OK')
        self.assertEqual(self.cr0(b'set', b'c', b'c'), b'OK')
        self.assertEqual(self.cr0(b'set', b'{a}b', b'd'), b'OK')
        with self.assertRaises(RedisError):
            self.mp.database(1).command(b'set', b'a', 'b')

    def test_outoforder(self):
        c1 = self.c0(b'set', b'a', b'a')
        c2 = self.c0(b'set', b'b', b'b')
        c3 = self.c0(b'set', b'c', b'c')
        c4 = self.c0(b'set', b'{a}b', b'ab')
        with self.assertRaises(RedisError):
            self.mp.database(1).command(b'set', b'a', 'b')
        self.assertEqual(c1(), b'OK')
        self.assertEqual(c2(), b'OK')
        self.assertEqual(c3(), b'OK')
        self.assertEqual(c4(), b'OK')

    def test_misc(self):
        # This tests an command which redis server says keys start in index 2.
        self.cr0(b'object', b'help')
        # Check command with no keys
        self.cr0(b'client', b'list')

    def test_server(self):
        self.assertEqual(self.cr0(b'set', b'aa', b'a'), b'OK')
        self.assertEqual(self.cr0(b'set', b'bb', b'b'), b'OK')
        self.assertEqual(self.cr0(b'set', b'cc', b'c'), b'OK')
        result = self.mp.run_commandreply_on_all_masters(b'KEYS', b'*')
        self.assertEqual(len(result), 3)
        result = list(result.values())
        result = [i for s in result for i in s]
        self.assertEqual(set(result), set([b'aa', b'bb', b'cc']))

    def test_moved(self):
        self.assertEqual(self.cr0(b'set', b'aa', b'a'), b'OK')
        self.assertEqual(self.cr0(b'set', b'bb', b'b'), b'OK')
        self.assertEqual(self.cr0(b'set', b'cc', b'c'), b'OK')
        result = self.mp.run_commandreply_on_all_masters(b'GET', b'aa')
        self.assertEqual(len(result), 3)
        result = list(result.values())
        self.assertEqual(result, [b'a', b'a', b'a'])

    def test_multi(self):
        with self.mp.database().multi() as m:
            cmd1 = m.command(b'SET', b'a', b'b')
            cmd2 = m.command(b'GET', b'a')
        self.assertEqual(cmd1(), b'OK')
        self.assertEqual(cmd2(), b'b')

    def test_eval(self):
        self.assertEqual(self.cr0('eval', "return redis.call('get',ARGV[1])", 0, 'evaltest'), None)

"""
class TestPubSub(unittest.TestCase):
    def setUp(self):
        self.server = RedisServer()
        self.mp = Multiplexer({'endpoints': ('localhost', self.server.port)})

    def tearDown(self):
        self.mp = None
        self.server = None

    def test_basic_pubsub(self):
        pubsub = self.mp.pubsub()
        cr = self.mp.database().commandreply
        pubsub.add('hi', 'bye')
        self.assertEqual(pubsub.message(), [b'subscribe', b'hi', 1])
        self.assertEqual(pubsub.message(), [b'psubscribe', b'bye', 2])
        self.assertEqual(pubsub.message(0.1), None)
        cr(b'PUBLISH', 'hi', 'there')
        self.assertEqual(pubsub.message(0.1), [b'message', b'hi', b'there'])
        cr(b'PUBLISH', 'bye', 'there')
        self.assertEqual(pubsub.message(0.1), [b'pmessage', b'bye', b'bye', b'there'])
        pubsub.ping()
        self.assertEqual(pubsub.message(), [b'pong', b''])
        pubsub.ping('hi')
        self.assertEqual(pubsub.message(0.1), [b'pong', b'hi'])
        pubsub.remove('hi')
"""

class TestNetwork(unittest.TestCase):
    class RedisServer(object):
        # Open in a new thread
        # with Hiredis
        def __init__(self, port):
            pass

        def get_bulk(self):
            pass

        def send_bulk(self):
            pass


if __name__ == '__main__':
    unittest.main()
