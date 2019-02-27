#TODO check exception error msgs (https://github.com/cloudant/python-cloudant/issues/80)
import unittest
from justredis import Multiplexer, RedisError, RedisReplyError
from justredis.justredis import Connection
from .redis_server import RedisServer, start_cluster
import socket

# TODO add connect error
class SocketWithIOErrorsManager(object):
    def __init__(self):
        self._errors = {}
        self._sockets = set()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def add_socket(self, socket):
        self._sockets.add(socket)
        socket._errors = dict(self._errors)

    def set_recv_error(self, times, counter, exception):
        self._errors['recv'] = [times, counter, counter, exception]
        for socket in self._sockets:
            socket._errors['recv'] = [times, counter, counter, exception]

    def set_send_error(self, times, counter, exception):
        self._errors['send'] = [times, counter, counter, exception]
        for socket in self._sockets:
            socket._errors['send'] = [times, counter, counter, exception]
    
    def check_error(self, data):
        if data:
            times, counter_max, counter, exception = data
            if counter == 0:
                if times == 0:
                    del data[:]
                else:
                    data[:] = [times, counter_max, counter_max, exception]
                raise exception
            else:
                data[:] = [times, counter_max, counter - 1, exception]


class SocketWithIOErrors(object):
    def __init__(self, socket, manager):
        self._socket = socket
        self._errors = {}
        self._manager = manager
        manager.add_socket(self)

    def __getattr__(self, name):
        return getattr(self._socket, name)

    def recv_into(self, *args, **kwargs):
        self._manager.check_error(self._errors.get('recv'))
        return self._socket.recv_into(*args, **kwargs)

    def sendall(self, *args, **kwargs):
        self._manager.check_error(self._errors.get('send'))
        return self._socket.sendall(*args, **kwargs)


def create_IOErrorConnection(manager):
    class IOErrorConnection(Connection):
        def __init__(self, *args, **kwargs):
            super(IOErrorConnection, self).__init__(*args, **kwargs)
            self.socket = SocketWithIOErrors(self.socket, manager)
    return IOErrorConnection


#class TestIOError(unittest.TestCase):
class A():
    def setUp(self):
        self.server = RedisServer()
        self.manager = SocketWithIOErrorsManager()
        self.mp = Multiplexer({'endpoints': ('localhost', self.server.port), 'connectionhandler': create_IOErrorConnection(self.manager)})
    
    def tearDown(self):
        self.server = None

    def test_simple(self):
        c = self.mp.database(0).command
        with self.manager as manager:
            manager.set_recv_error(1, 1, Exception('IO'))
            cmd = c(b'SET', b'a', b'b')
            self.assertEqual(cmd(), b'OK')
            cmd = c(b'GET', b'a')
            self.assertEqual(cmd(), b'b')


class TestServerWithPassword(unittest.TestCase):
    def setUp(self):
        self.server = RedisServer(extraparams='--requirepass blah')
        self.mp = Multiplexer({'endpoints': ('localhost', self.server.port), 'password': 'blah'})
    
    def tearDown(self):
        self.server = None
        self.mp = None

    def test_wrongpassword(self):
        mp = Multiplexer({'endpoints': ('localhost', self.server.port), 'password': 'wrong'})
        with self.assertRaises(RedisReplyError):
            mp.database(0).command(b'GET', b'a')()
        mp = Multiplexer({'endpoints': ('localhost', self.server.port)})
        with self.assertRaises(RedisReplyError):
            mp.database(0).command(b'GET', b'a')()

    def test_simple(self):
        c = self.mp.database(0).command
        cmd = c(b'SET', b'a', b'b')
        self.assertEqual(cmd(), b'OK')
        cmd = c(b'GET', b'a')
        self.assertEqual(cmd(), b'b')

#    def test_notallowed(self):
#        c = self.mp.database(0).command
#        cmd = c(b'AUTH', b'asd')
#        self.assertEqual(cmd(), b'OK')

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


class TestCluster(unittest.TestCase):
    def setUp(self):
        self.servers = start_cluster(3)
        self.mp = Multiplexer({'endpoints': ('localhost', self.servers[0].port)})

    def tearDown(self):
        self.mp = None
        self.servers = None

    def test_basic(self):
        c = self.mp.database().command
        cmd = c(b'set', b'a', b'b')
        self.assertEqual(cmd(), b'OK')
        cmd = c(b'set', b'b', b'b')
        self.assertEqual(cmd(), b'OK')
        cmd = c(b'set', b'c', b'b')
        self.assertEqual(cmd(), b'OK')


if __name__ == '__main__':
    unittest.main()
