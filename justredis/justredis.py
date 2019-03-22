from __future__ import absolute_import
import sys
from collections import deque
from hashlib import sha1
# binascii requires python to be compiled with zlib ?
from binascii import crc_hqx

import hiredis


# Execution environment
from .environment import get_env
Lock, Event, socket, select, thread, Queue = get_env("Lock", "Event", "socket", "select", "thread", "Queue")


# Exceptions
# An error response from the redis server
class RedisReplyError(Exception):
    pass


# An error from this library
class RedisError(Exception):
    pass


# Python 2/3 compatability
if isinstance(str(1), bytes):
    def encode_number(number):
        return str(number)
else:
    def encode_number(number):
        return str(number).encode()


try:
    long
except NameError:
    long = int
try:
    unicode
except NameError:
    unicode = str


try:
    connect_errors = (ConnectionError, socket.error, socket.timeout)
except NameError:
    connect_errors = (socket.error, socket.timeout)


# TODO (question) Am I missing an exception type here ?
socket_errors = (IOError, OSError, socket.error, socket.timeout)


platform = ''
if sys.platform.startswith('linux'):
    platform = 'linux'
elif sys.platform.startswith('darwin'):
    platform = 'darwin'
elif sys.platform.startswith('win'):
    platform = 'windows'


# TODO (question) what other commands to put here ?
not_allowed_commands = {b'WATCH', b'BLPOP', b'MULTI', b'EXEC', b'DISCARD', b'BRPOP', b'AUTH', b'SELECT', b'SUBSCRIBE', b'PSUBSCRIBE', b'UNSUBSCRIBE', b'PUNSUBSCRIBE', b'MONITOR'}


# Basic encoder
def encode(encoding='utf-8', errors='strict'):
    def encode_with_encoding(inp, encoding=encoding, errors=errors):
        if isinstance(inp, bytes):
            return inp
        elif isinstance(inp, unicode):
            return inp.encode(encoding, errors)
        elif isinstance(inp, bool):
            raise ValueError('Invalid input for encoding')
        elif isinstance(inp, (int, long)):
            return str(inp).encode()
        elif isinstance(inp, float):
            return repr(inp).encode()
        raise ValueError('Invalid input for encoding')
    return encode_with_encoding

utf8_encode = encode()


# Basic decoder
def bytes_as_strings(encoding='utf-8', errors='strict'):
    def bytes_as_strings_with_encoding(inp, encoding=encoding, errors=errors):
        if isinstance(inp, bytes):
            return inp.decode(encoding, errors)
        elif isinstance(inp, list):
            return [bytes_as_strings_with_encoding(x, encoding, errors) for x in inp]
        return inp
    return bytes_as_strings_with_encoding

utf8_bytes_as_strings = bytes_as_strings()

# Redis protocol encoder / decoder
def encode_command(data, encoder):
    output = [b'*', encode_number(len(data)), b'\r\n']
    for arg in data:
        arg = encoder(arg)
        output.extend([b'$', encode_number(len(arg)), b'\r\n', arg, b'\r\n'])
    return output


def chunk_encoded_command(cmd, chunk_size):
    data = []
    data_len = 0
    for x in cmd.encode():
        chunk_len = len(x)
        if data_len > chunk_size or chunk_len > chunk_size:
            yield b''.join(data)
            if chunk_len > chunk_size:
                yield x
                data = []
                data_len = 0
            else:
                data = [x]
                data_len = chunk_len
        else:
            data.append(x)
            data_len += chunk_len
    if data:
        yield b''.join(data)


def chunk_encoded_commands(cmds, chunk_size):
    data = []
    data_len = 0
    cmd_index = 0
    for cmd in cmds:
        for x in cmd.encode():
            chunk_len = len(x)
            if data_len > chunk_size or chunk_len > chunk_size:
                yield b''.join(data)
                if chunk_len > chunk_size:
                    yield x
                    data = []
                    data_len = 0
                else:
                    data = [x]
                    data_len = chunk_len
            else:
                data.append(x)
                data_len += chunk_len
    if data:
        yield b''.join(data)


def hiredis_parser():
    reader = hiredis.Reader()
    while True:
        try:
            res = reader.gets()
        except hiredis.ProtocolError:
            raise RedisError(*e.args)
        if isinstance(res, hiredis.ReplyError):
            res = RedisReplyError(*res.args)
        data = yield res
        if data:
            reader.feed(*data)


# Cluster hash calculation
def calc_hash(key):
    try:
        s = key.index(b'{')
        e = key.index(b'}')
        if e > s + 1:
            key = key[s + 1:e]
    except ValueError:
        pass
    return crc_hqx(key, 0) % 16384


# TODO better handling for unix domain, and default ports in tuple / list
def parse_uri(uri):
    endpoints = []
    config = {}
    if isinstance(uri, dict):
        config = uri
        config_endpoints = config.pop('endpoints', None)
        if config_endpoints:
            uri = config_endpoints
        else:
            uri = None
    if uri is None:
        endpoints = [('localhost', 6379)]
    elif isinstance(uri, str):
        if '/' in uri:
            endpoints = [uri]
        else:
            endpoints = [(uri, 6379)]
    elif isinstance(uri, tuple):
        endpoints = [uri]
    elif isinstance(uri, list):
        endpoints = uri
    return endpoints, config


def parse_command(source, *args, **kwargs):
    if not args:
        raise RedisError('Empty command not allowed')
    # TODO First argument should always be utf-8 encoded english?
    cmd = args[0]
    cmd = (cmd if isinstance(cmd, bytes) else cmd.encode()).upper()
    if cmd in not_allowed_commands:
        raise RedisError('Command is not allowed')
    encoder = source._encoder
    decoder = source._decoder
    throw = True
    retries = source._retries
    server = source._server
    if kwargs:
        encoder = kwargs.get('encoder', encoder)
        decoder = kwargs.get('decoder', decoder)
        throw = kwargs.get('throw', throw)
        retries = kwargs.get('retries', retries)
    # Handle script caching
    if cmd == b'EVAL':
        script = args[1]
        sha = source._scripts.get(script)
        if sha:
            args = (b'EVALSHA', sha) + args[2:]
        else:
            enc = encoder or utf8_encode
            bscript = script if isinstance(script, bytes) else enc(script)
            sha = sha1(bscript).hexdigest()
            source._scripts[script] = sha
            source._scripts_sha[sha] = script
    return Command(args, source, encoder, decoder, throw, retries, True, server)


class Transport(object):
    __slots__ = '_buffer', '_socket'

    def __init__(self, configuration):
        buffersize = configuration.get('recvbuffersize', 16384)
        self._buffer = bytearray(buffersize)

    @classmethod
    def create(cls, endpoint, configuration):
        raise NotImplementedError()
    
    def read(self):
        length = self._socket.recv_into(self._buffer)
        if length == 0:
            raise RedisError('Connection closed')
        return self._buffer, 0, length

    def write(self, data):
        self._socket.sendall(data)

    def ready(self, timeout=None):
        res = select([self._socket], [], [], timeout)
        return True if res[0] else False

    def name(self):
        res = self._socket.getpeername()
        if self._socket.family == socket.AF_INET6:
            res = res[:2]
        return res

    def close(self):
        try:
            self._socket.shutdown(socket.SHUT_RDWR)
        except Exception:
            pass
        try:
            self._socket.close()
        except Exception:
            pass
        self._socket = None
        self._buffer = None


class UnixTransport(Transport):
    @classmethod
    def create(cls, endpoint, configuration):
        connecttimeout = configuration.get('connecttimeout', socket.getdefaulttimeout())
        sockettimeout = configuration.get('sockettimeout', socket.getdefaulttimeout())
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(connecttimeout)
        sock.connect(endpoint)
        sock.settimeout(sockettimeout)
        obj = cls(configuration)
        obj._socket = sock
        return obj


class TcpTransport(Transport):
    @classmethod
    def create(cls, endpoint, configuration):
        connecttimeout = configuration.get('connecttimeout', socket.getdefaulttimeout())
        sockettimeout = configuration.get('sockettimeout', socket.getdefaulttimeout())
        tcpkeepalive = configuration.get('tcpkeepalive', 300)
        tcpnodelay = configuration.get('tcpnodelay', False)
        sock = socket.create_connection(endpoint, timeout=connecttimeout)
        sock.settimeout(sockettimeout)
        if isinstance(sock.getsockname(), tuple):
            if tcpnodelay:
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            if tcpkeepalive:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                if platform == 'linux':
                    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, tcpkeepalive)
                    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, tcpkeepalive // 3)
                    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3)
                elif platform == 'darwin':
                    sock.setsockopt(socket.IPPROTO_TCP, 0x10, tcpkeepalive // 3)
                elif platform == 'windows':
                    sock.ioctl(SIO_KEEPALIVE_VALS, (1, tcpkeepalive * 1000, tcpkeepalive // 3 * 1000))
        obj = cls(configuration)
        obj._socket = sock
        return obj


class Protocol(object):
    __slots__ = '_transport', '_parser', '_chunk_send_size'

    def __init__(self, transport):
        self._transport = transport
        self._parser = hiredis_parser()
        self._parser.send(None)
        self._chunk_send_size = 6000

    def read(self, allow_empty=False):
        res = self._parser.send(None)
        while res is False:
            if allow_empty:
                return res
            data, offset, length = self._transport.read()
            if length == 0:
                raise RedisError('Connection closed')
            res = self._parser.send((data, 0, length))
        return res
    
    def write(self, cmd):
        for x in cmd.stream(self._chunk_send_size):
            self._transport.write(x)

    def close(self):
        try:
            self._transport.close()
        except Exception:
            pass
        self._transport = None
        self._parser = None


# TODO add ony_way (if no commands in list, put it in some result list ?)
# TODO what is retry connect strategy ?
class Connection(object):
#    __slots__ = '_transport', 'name', 'commands', 'closed', 'read_lock', 'send_lock', 'chunk_send_size', 'parser', 'lastdatabase', 'select', 'thread_event', 'thread'

    @classmethod
    def create(cls, endpoint, configuration):
        connectretry = configuration.get('connectretry', 0) + 1
        transport_class = configuration.get('transport', TcpTransport)
        while connectretry:
            try:
                transport = transport_class.create(endpoint, configuration)
                break
            except connect_errors as e:
                connectretry -= 1
                if not connectretry:
                    raise RedisError('Connection failed: %r' % e)
        connection = cls(transport)
        # Password must be bytes or utf-8 encoded string
        password = configuration.get('password')
        if password is not None:
            cmd = Command((b'AUTH', password))
            connection.send(cmd)
            cmd()
        return connection

    def __init__(self, transport):
        self._transport = transport
        self._protocol = Protocol(transport)
        self.name = transport.name
        self.closed = False
        self._lastdatabase = 0
        self._read_lock = Lock('read')
        self._send_lock = Lock('send')
        self._commands = deque()
        if thread:
            self._thread = thread(self.loop)

    # This code should be exception safe
    # TODO WHAT TO DO ABOUT PUBSUB ?
    def loop(self):
        while self.read():
            pass

    def close_in_lock(self):
        self.closed = True
        try:
            self._protocol.close()
        except Exception:
            pass
        self._transport = None
        self._protocol = None

    def close(self, e=None):
        with self._read_lock:
            with self._send_lock:
                e = e or RedisError('Connection closed')
                for command in self._commands:
                    # This are already sent and we don't know if they happened or not.
                    command.set_result(e, dont_retry=True)
                self._commands.clear()

    # TODO We dont set TCP NODELAY to give it a chance to coalese multiple sends together, another option might be to queue them together here
    def send(self, cmd):
        if cmd._got_result:
            raise RedisError('Command already processed')
        should_close = False
        with self._send_lock:
            try:
                if self.closed:
                    raise RedisError('Connection closed')
                db_number = cmd.get_number()
                if db_number is not None and db_number != self._lastdatabase:
                    select_cmd = Command((b'SELECT', db_number))
                    self._commands.append(select_cmd)
                    self._protocol.write(select_cmd)
                    self._lastdatabase = db_number
                if cmd._asking:
                    asking_cmd = Command((b'ASKING', ))
                    self._commands.append(asking_cmd)
                    self._protocol.write(asking_cmd)
                    # TODO move this only if sent?
                    cmd._asking = False
                # pub/sub commands do not expect a result (mark this as one-way connection so others dont try)
                if cmd.set_resolver(self):
                    # TODO enforce this can't be in one way mode
                    self._commands.append(cmd)
                    # We send after adding to command list to make use of blocking I/O of the other read side
                    try:
                        self._protocol.write(cmd)
                    except Exception:
                        # Need to do this while still holding the lock
                        self._commands.pop()
                        raise
                else:
                    self._protocol.write(cmd)
            except socket_errors as e:
                should_close = True
                self.close_in_lock()
                cmd.set_result(e, dont_retry=True)
            except Exception as e:
                cmd.set_result(e, dont_retry=True)
        if should_close:
            self.close()

    def read(self, calling_cmd=None):
        should_close = False
        cmd = None
        with self._read_lock:
            try:
                if calling_cmd and calling_cmd._got_result:
                    return False
                first_read = self._protocol.read()
                try:
                    cmd = self._commands.popleft()
                except IndexError:
                    # This is a one-way connection
                    return first_read
                num_results = cmd.how_many_results()
                # self._protocol cannot be None here (because of locking)
                results = [self._protocol.read() for _ in range(num_results - 1)]
                results.insert(0, first_read)
                cmd.set_result(results)#, dont_retry=True)
            except Exception as e:
                should_close = True
                self.close_in_lock()
                if cmd:
                    cmd.set_result(e, dont_retry=True)
        if should_close:
            self.close()


class Command(object):
#    __slots__ = '_data', '_database', '_resolver', '_encoder', '_decoder', '_throw', '_got_result', '_result', '_retries', '_enqueue', '_server', '_asking'

    def __init__(self, data, database=None, encoder=None, decoder=None, throw=True, retries=3, enqueue=True, server=None):
        self._data = data
        self._database = database
        self._encoder = encoder or utf8_encode
        self._decoder = decoder
        self._throw = throw
        self._retries = retries
        self._enqueue = enqueue
        self._server = server
        self._resolver = None
        self._got_result = False
        self._result = None
        self._asking = False

    def get_number(self):
        return self._database._number if self._database else None
    
    # TODO use this all over the code
    # TODO replace the _data with the already encoded data ?
    def get_index_as_bytes(self, index):
        return self._encoder(self._data[index])

    def how_many_results(self):
        return 1

    def stream(self, chunk_size):
        return chunk_encoded_command(self, chunk_size)

    def encode(self):
        return encode_command(self._data, self._encoder)

    def set_resolver(self, connection):
        if self._enqueue:
            self._resolver = connection
        return self._enqueue

    def set_result(self, result, dont_retry=False):
        try:
            if not isinstance(result, Exception):
                result = result[0]
            if self._database and not dont_retry and self._retries and isinstance(result, Exception):
                self._retries -= 1
                # We will only retry if it's a network I/O or some redis logic
                if isinstance(result, socket_errors):
                    self._database._multiplexer._send_command(self)
                    return
                elif isinstance(result, RedisReplyError):
                    # hiredis exceptions are already encoded....
                    if result.args[0].startswith('NOSCRIPT'):
                        sha = self._data[1]
                        script = self._database._scripts_sha.get(sha)
                        if script:
                            self._data = (b'EVAL', script) + self._data[2:]
                            self._database._multiplexer._send_command(self)
                            return
                    elif result.args[0].startswith('MOVED'):
                        _, hashslot, addr = result.args[0].split(' ')
                        hashslot = int(hashslot)
                        addr = addr.rsplit(':', 1)
                        addr = (addr[0], int(addr[1]))
                        self._database._multiplexer._update_slots(moved_hint=(hashslot, addr))
                        self._server = addr
                        self._database._multiplexer._send_command(self)
                        return
                    elif result.args[0].startswith('ASKING'):
                        _, hashslot, addr = result.args[0].split(' ')
                        hashslot = int(hashslot)
                        addr = addr.rsplit(':', 1)
                        addr = (addr[0], int(addr[1]))
                        self._asking = True
                        self._server = addr
                        self._database._multiplexer._send_command(self)
                        return
            # TODO should we call decoder on Exceptions as well ?
            self._result = result if not self._decoder else self._decoder(result)
            self._got_result = True
            self._data = None
            self._database = None
            self._resolver = None
        except Exception as e:
            self._result = e
            self._got_result = True
            self._data = None
            self._database = None
            self._resolver = None

    def __call__(self):
        if not self._got_result and not self._resolver:
            raise RedisError('Command is not finalized yet')
        # This is a loop because of multi threading.
        while not self._got_result:
#            if thread:
#                print(1, self)
#                import time; time.sleep(1)
#                try:
#                    with self._resolver._read_lock:
#                        pass
#                except:
#                    pass
#                    print(2)
#                print(3)
#                self._resolver.read(self)
#            else:
                self._resolver.read(self)
        if self._throw and isinstance(self._result, Exception):
            raise self._result
        return self._result


class Multiplexer(object):
    def __init__(self, configuration=None):
        self._endpoints, self._configuration = parse_uri(configuration)
        self._connection = None
        self._lock = Lock('multiplexer')
        self._scripts = {}
        self._scripts_sha = {}
        self._clustered = False
    
    def close(self):
        print(3)
        if self._connection:
            with self._lock:
                if self._connection:
                    self._connection.close()
                    self._connection = None
    
    def __del__(self):
        self.close()

    def database(self, number=0, encoder=None, decoder=None, retries=3, server=None):
        if number != 0 and self._clustered and server is None:
            raise RedisError('Redis cluster has no database selection support')
        return Database(self, number, encoder, decoder, retries, server)

    def _send_command(self, cmd):
#        print(cmd._data)
        if not self._connection or self._connection.closed:
            with self._lock:
                if not self._connection or self._connection.closed:
                    print(1)
                    self._connection = Connection.create(self._endpoints[0], self._configuration)
        print(2)
        self._connection.send(cmd)


class MultiplexerComplex(object):
    __slots__ = '_endpoints', '_configuration', '_connections', '_last_connection', '_scripts', '_scripts_sha', '_pubsub', '_lock', '_clustered', '_command_cache', '_already_asking_for_slots', '_slots'

    def __init__(self, configuration=None):
        self._endpoints, self._configuration = parse_uri(configuration)
        self._connections = {}
        self._last_connection = None
        # The reason we do double dictionary here is for faster lookup in case of lookup failure and multi threading protection
        self._scripts = {}
        self._scripts_sha = {}
        self._pubsub = None
        self._lock = Lock()
        self._clustered = None
        self._command_cache = {}
        self._already_asking_for_slots = False
        self._slots = []
#        for endpoint in self._endpoints:
#            # Since we don't actually connect here, the endpoint name might be different but we don't care
#            self._connections[endpoint] = Connection(endpoint, self._configuration)

    # TODO have a closed flag, and stop requesting new connections? (no, it should be reusable?)
    def close(self):
        with self._lock:
            for connection in self._connections.values():
                try:
                    connection.close()
                except Exception:
                    pass
            self._connections = {}
            self._last_connection = None

    # TODO (question) is this sane or we should not support this (explicitly call close?)
    def __del__(self):
        self.close()

    def database(self, number=0, encoder=None, decoder=None, retries=3, server=None):
        if number != 0 and self._clustered and server is None:
            raise RedisError('Redis cluster has no database selection support')
        return Database(self, number, encoder, decoder, retries, server)

    # TODO support optional server instance for pub/sub ? (for keyspace notification at least)
    def pubsub(self, encoder=None, decoder=None):
        if self._pubsub is None:
            with self._lock:
                if self._pubsub is None:
                    self._pubsub = PubSub(self)
        return PubSubInstance(self._pubsub, encoder, decoder)

    def endpoints(self):
        if self._clustered is None:
            self._update_slots()
        # TODO is this atomic ?
        return list(self._connections.keys())

    def run_commandreply_on_all_masters(self, *args, **kwargs):
        return self.run_commandreply_on_all('masters', *args, **kwargs)

    def run_commandreply_on_all(self, who='masters', *args, **kwargs):
        if self._clustered is False:
            raise RedisError('This command only runs on cluster mode')
        if who != 'masters':
            raise RedisError('Currently only masters supported')
        res = {}
        for endpoint in self.endpoints():
            try:
                res[endpoint] = self.database(server=endpoint).commandreply(*args, **kwargs)
            except Exception as e:
                res[endpoint] = e
        return res

    def _get_connection(self, hashslot=None):
        if not hashslot:
            if self._last_connection is None or self._last_connection.closed:
                with self._lock:
                    if self._last_connection is None or self._last_connection.closed:
                        connection_list = list(self._connections.values())
                        for connection in connection_list:
                            # TODO can connection be None somehow ?
                            if not connection.closed:
                                self._last_connection = connection
                                break
                        else:
                            address_list = list(self._connections.keys()) or self._endpoints
                            for address in address_list:
                                try:
                                    connection = self._connections.get(address)
                                    if not connection or connection.closed:
                                        self._last_connection = connection = Connection.create(address, self._configuration)
                                        self._connections[connection.name] = connection
                                        break
                                except Exception as e:
                                    exc = e
                            else:
                                raise exc
                        # If a server is not answering, update my world view ?
                        self._update_slots(with_connection=connection)
            return self._last_connection
        else:
            # We don't have to update _slots here, since the call to _get_command_from_cache (which is a prequesite) already updated it if needed.
            for slot in self._slots:
                if hashslot <= slot[0]:
                    break
            addr = slot[1]
            try:
                conn = self._connections.get(addr)
                if not conn or conn.closed:
                    # TODO do we want here a lock per connection ?
                    with self._lock:
                        conn = self._connections.get(addr)
                        if conn is None or conn.closed:
                            conn = Connection.create(addr, self._configuration)
                            self._connections[addr] = conn
                return conn
            # TODO (misc) should we try getting another connection here ?
            except Exception:
                self._update_slots()
                raise

    # TODO check if closed and dont allow
    def _send_command(self, cmd):
        server = cmd._server
        if not server:
            # In multi, all commands must be in the same hashslot, so look for the first one
            if isinstance(cmd, MultiCommand) and self._clustered is not False:
                for a_cmd in cmd._cmds[1:-1]:
                    hashslot = self._get_command_from_cache(a_cmd)
                    if hashslot is not None:
                        break
            else:
                hashslot = self._get_command_from_cache(cmd)
            connection = self._get_connection(hashslot)
        else:
            try:
                conn = self._connections.get(server)
                if not conn:
                    #TODO do we want here a lock per connection ?
                    with self._lock:
                        conn = self._connections.get(server)
                        if not conn or conn.closed:
                            conn = Connection.create(server, self._configuration)
                            # We only put the server back if it's relevent
                            if server in self._connections:
                                self._connections[server] = conn
                connection = conn
            # TODO (misc) should we try getting another connection here ?
            except Exception:
                self._update_slots()
                # TODO not raise here, but cmd.set_result which will trigger a retry ? (and catch _update_slots error as well?)
                raise
        connection.send(cmd)

    def _update_slots(self, moved_hint=None, with_connection=None):
        if self._clustered is False:
            return
        if self._already_asking_for_slots:
            return
        # We might already know about this move after this command was originally issued, from other commands
        if moved_hint and self._slots:
            hashslot, addr = moved_hint
            for slot in self._slots:
                if hashslot <= slot[0]:
                    break
            # We already know this, don't retry getting a new world view
            if addr == slot[1]:
                return
        try:
            # multiple commands can trigger this while in queue so we make sure to do it once
            self._already_asking_for_slots = True
            # TODO should we retry on some type of errors ?
            cmd = Command((b'CLUSTER', b'SLOTS'))
            # TODO with_connection should be forced ?
            connection = with_connection or self._get_connection()
            connection.send(cmd)
            try:
                slots = cmd()
                self._clustered = True
            except RedisReplyError:
                slots = []
                self._clustered = False
            slots.sort(key=lambda x: x[0])
            slots = [(x[1], (x[2][0].decode(), x[2][1])) for x in slots]
            # resync connections list with slot list
            # TODO should we lock connections while updating ? recursive lock ?
            if self._clustered:
                current_addresses = set(self._connections)
                new_addresses = set([x[1] for x in slots])
                for address in current_addresses ^ new_addresses:
                    if address in current_addresses and address not in new_addresses:
                        try:
                            connections = self._connections.pop(entry)
                            connections.close()
                        except Exception:
                            pass
                    elif address not in current_addresses and address in new_addresses:
                        self._connections.setdefault(address, None)
            else:
                for address in set(self._connections):
                    if connection.name != address:
                        try:
                            connections = self._connections.pop(entry)
                            connections.close()
                        except Exception:
                            pass
                if connection.name not in self._connections:
                    self._connection[connection.name] = connection
            self._slots = slots
        finally:
            self._already_asking_for_slots = False

    # It is unreasonable to expect the user to provide us with the key index for each command so we ask the server and cache it
    def _get_command_from_cache(self, cmd):
        if self._clustered is False:
            return None
        command_name = cmd.get_index_as_bytes(0)
        keyindex = self._command_cache.get(command_name)
        if keyindex is None:
            # Requires redis server 2.8.13 or above
            # TODO allow retries? might be infinite loop...
            info_cmd = Command((b'COMMAND', b'INFO', command_name))
            # If connection is dead, then the initiator command should retry
            self._get_connection().send(info_cmd)
            # We do this check here, since _get_connection first try will trigger a clustered check and we need to abort if it's not (to not trigger the send_command's hashslot)
            if self._clustered is False:
                return None
            cmdinfo = info_cmd()[0]
            # If the server does not know the command, we can't redirect it properly in cluster mode
            keyindex = cmdinfo[3] if cmdinfo else 0
            # TODO add other movablekey commands
            if cmdinfo[0] in (b'eval', b'evalsha'):
                keyindex = 3
            elif cmdinfo[0] in (b'zunionstore', b'zinterstore'):
                keyindex = 1
            self._command_cache[command_name] = keyindex
        if keyindex == 0:
            return None
        try:
            key = cmd.get_index_as_bytes(keyindex)
        except IndexError:
            return None
        return calc_hash(key)


class Database(object):
    __slots__ = '_multiplexer', '_number', '_encoder', '_decoder', '_retries', '_scripts', '_scripts_sha', '_server'

    def __init__(self, multiplexer, number, encoder, decoder, retries, server):
        self._multiplexer = multiplexer
        self._number = number
        self._encoder = encoder
        self._decoder = decoder
        self._retries = retries
        self._server = server
        self._scripts = multiplexer._scripts
        self._scripts_sha = multiplexer._scripts_sha

    def command(self, *args, **kwargs):
        cmd = parse_command(self, *args, **kwargs)
        self._multiplexer._send_command(cmd)
        return cmd

    def commandreply(self, *args, **kwargs):
        return self.command(*args, **kwargs)()

    def multi(self, retries=None):
        return MultiCommand(self, retries if retries is not None else self._retries, server=self._server)


# TODO maybe split the user facing API from the internal Command one (atleast mark it as _)
# To know the result of a multi command simply resolve any command inside
class MultiCommand(object):
#    __slots__ = '_database', '_retries', '_server', '_cmds', '_done', '_asking', '_got_result'

    def __init__(self, database, retries, server):
        self._database = database
        self._retries = retries
        self._server = server
        self._cmds = []
        self._done = False
        self._asking = False
        self._got_result = False

    def stream(self, chunk_size):
        return chunk_encoded_commands(self._cmds, chunk_size)

    def set_resolver(self, connection):
        for cmd in self._cmds:
            cmd._resolver = connection
        return True

    def get_number(self):
        return self._database._number

    def set_result(self, result, dont_retry=False):
        # TODO (question) If there is an exec error, maybe preserve the original per-cmd error as well ?
        if isinstance(result, list):
            exec_res = result[-1]
            if isinstance(exec_res, list):
                for cmd, res in zip(self._cmds[1:-1], exec_res):
                    cmd.set_result([res], dont_retry=True)
                self._database = None
                self._cmds = None
                return
        else:
            exec_res = result
        
        if self._database and not dont_retry and self._retries and isinstance(exec_res, Exception):
            self._retries -= 1
            if not isinstance(exec_res, RedisReplyError):
                self._database._multiplexer._send_command(self)
                return

        for cmd in self._cmds[1:-1]:
            cmd.set_result(exec_res, dont_retry=True)
        self._got_result = True
        self._database = None
        self._cmds = None

    def how_many_results(self):
        return len(self._cmds)

    def __enter__(self):
        if self._done:
            raise RedisError('Multiple command already finished')
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if not exc_type:
            self.execute(True)
        else:
            self.discard(True)

    def discard(self, soft=False):
        if self._done:
            if soft:
                return
            raise RedisError('Multiple command already finished')
        for cmd in self._cmds:
            cmd.set_result(RedisError('Multiple command aborted'), dont_retry=True)
        self._cmds = []
        self._database = None
        self._done = True

    def execute(self, soft=False):
        if self._done:
            if soft:
                return
            raise RedisError('Multiple command already finished')
        self._done = True
        if self._cmds:
            m_cmd = Command((b'MULTI', ), self._database)
            e_cmd = Command((b'EXEC', ), self._database)
            self._cmds.insert(0, m_cmd)
            self._cmds.append(e_cmd)
            self._database._multiplexer._send_command(self)

    def command(self, *args, **kwargs):
        if self._done:
            raise RedisError('Multiple command already finished')
        cmd = parse_command(self._database, *args, **kwargs)
        self._cmds.append(cmd)
        return cmd


# TODO the multiplexer .close should close this as well ?
# TODO document the error model possible here and implications
# TODO auto close by looking at the reply number of subs ? (nop)
class PubSub(object):
    __slots__ = '_multiplexer', '_connection', '_lock', '_msg_lock', '_msg_waiting', '_registered_instances', '_registered_channels', '_registered_patterns', '_thread'

    def __init__(self, multiplexer):
        self._multiplexer = multiplexer
        self._connection = None
        self._lock = Lock()
        self._msg_lock = Lock()
        self._msg_waiting = Event()
        self._registered_instances = {}
        self._registered_channels = {}
        self._registered_patterns = {}

    def create_connection(self):
        if self._connection is None or self._connection.closed:
            # TODO thread safety with _get_connection from multiplexer?
            # if empty...
            for endpoint in self._multiplexer.endpoints():
                try:
                    self._connection = Connection.create(endpoint, self._multiplexer._configuration, False)
                    if thread:
                        self._thread = thread(self.loop)
                    break
                except Exception as e:
                    exc = e
            else:
                raise exc
            return True
        return False

    def _command(self, cmd, *args):
        if self.create_connection():
            channels = self._registered_channels.keys()
            if channels:
                cmd = Command((b'SUBSCRIBE', ) + tuple(channels), enqueue=False, retries=0)
                self._connection.send(cmd)
            patterns = self._registered_patterns.keys()
            if patterns:
                cmd = Command((b'PSUBSCRIBE', ) + tuple(patterns), enqueue=False, retries=0)
                self._connection.send(cmd)
            ret = True
        # If it's a new connection, it will already run the command in the given list above
        else:
            cmd = Command((cmd, ) + args, enqueue=False, retries=0)
            self._connection.send(cmd)
            ret = False
        if not self._registered_channels and not self._registered_patterns:
            self._connection.close()
        return ret

    # TODO handle i/o error (reconnect / resubscribe with main lock, at start)
    # TODO should we handle the possibility that someone called message with nothing registered ?
    def message(self, instance, max_timeout=None):
        try:
            got_lock = self._msg_lock.acquire(False if instance is not None else True)
            if not got_lock:
                return self._msg_waiting.wait(max_timeout)
            self._msg_waiting.clear()
            res = self._connection.recv(allow_empty=True)
            if res is False:
                if self._connection.wait(max_timeout):
                    res = self._connection.recv()
                else:
                    res = None
            if res:
                self._msg_waiting.set()
                # The reason we send subscribe messages as well, is to know when an I/O reconnection has occurred
                if res[0] == b'message':
                    with self._lock:
                        for _instance in self._registered_channels[res[1]]:
                            _instance._add_message(res)
                elif res[0] == b'pmessage':
                    with self._lock:
                        for _instance in self._registered_patterns[res[1]]:
                            _instance._add_message(res)
                elif res[0] == b'subscribe':
                    with self._lock:
                        for _instance in self._registered_channels[res[1]]:
                            _instance._add_message(res)
                elif res[0] == b'psubscribe':
                    with self._lock:
                        for _instance in self._registered_patterns[res[1]]:
                            _instance._add_message(res)
                elif res[0] == b'pong':
                    with self._lock:
                        for _instance in self._registered_instances.keys():
                            _instance._add_message(res)
        finally:
            if got_lock:
                self._msg_lock.release()

    def loop(self):
        conn = self._connection
        while True:
            # Drop out for a new thread if there was an I/O error
            if self._connection != conn:
                return
            self.message(None)

    # TODO (question) should we deliver ping to everyone, or the instance who requested only ?
    def ping(self, message=None):
        with self._lock:
            if message:
                self._command(b'PING', message)
            else:
                self._command(b'PING')

    # If in register / unregister there is an I/O at least it's bookkeeped first so later invocations will fix it
    def register(self, instance, channels=None, patterns=None):
        with self._lock:
            registered = self._registered_instances.setdefault(instance, [set(), set()])
            if channels:
                registered[0].update(channels)
                for channel in channels:
                    self._registered_channels.setdefault(channel, set()).add(instance)
            if patterns:
                registered[1].update(patterns)
                for pattern in patterns:
                    self._registered_patterns.setdefault(pattern, set()).add(instance)
            if channels:
                # If this was a new connection, the channels were already registered
                if self._command(b'SUBSCRIBE', *channels):
                    return
            if patterns:
                self._command(b'PSUBSCRIBE', *patterns)

    def unregister(self, instance, channels=None, patterns=None):
        with self._lock:
            channels_to_remove = []
            patterns_to_remove = []
            registered_channels, registered_patterns = self._registered_instances[instance]
            for channel in registered_channels:
                if channels and channel not in channels:
                    continue
                registered_channel = self._registered_channels.get(channel)
                if registered_channel is None:
                    continue
                registered_channel.discard(instance)
                if not registered_channel:
                    channels_to_remove.append(channel)
            for pattern in registered_patterns:
                if patterns and pattern not in patterns:
                    continue
                registered_pattern = self._registered_patterns.get(pattern)
                if registered_pattern is None:
                    continue
                registered_pattern.discard(instance)
                if not registered_pattern:
                    patterns_to_remove.append(pattern)
            if not registered_channels and not registered_patterns:
                del self._registered_instances[instance]
            if channels_to_remove:
                # If this was a new connection, no need to unregister patterns
                if self._command(b'UNSUBSCRIBE', *channels_to_remove):
                    return
            if patterns_to_remove:
                self._command(b'PUNSUBSCRIBE', *patterns_to_remove)


# Don't pass this between different invocation contexts
class PubSubInstance(object):
    __slots__ = '_pubsub', '_encoder', '_decoder', '_closed', '_messages'

    def __init__(self, pubsub, encoder, decoder):
        self._pubsub = pubsub
        self._encoder = encoder or utf8_encode
        self._decoder = decoder
        self._closed = False
        self._messages = deque()

    def __del__(self):
        self.close()

    def close(self):
        if not self._closed:
            self._closed = True
            try:
                self._pubsub.unregister(self)
            except Exception:
                pass
            self._messages = None
            self._decoder = None
            self._encoder = None
            self._pubsub = None

    def add(self, channels=None, patterns=None):
        self._cmd(self._pubsub.register, channels, patterns)

    # TODO (question) should we removed the self._messages that are related to this channels and patterns ?
    def remove(self, channels=None, patterns=None):
        self._cmd(self._pubsub.unregister, channels, patterns)

    def message(self, max_timeout=None):
        if self._closed:
            raise RedisError('Pub/sub instance closed')
        try:
            return self._messages.popleft() if not self._decoder else self._decoder(self._messages.popleft())
        except IndexError:
            self._pubsub.message(self, max_timeout)
            try:
                return self._messages.popleft() if not self._decoder else self._decoder(self._messages.popleft())
            except IndexError:
                return None

    def ping(self, message=None):
        if self._closed:
            raise RedisError('Pub/sub instance closed')
        self._pubsub.ping(message)

    def _cmd(self, cmd, channels, patterns):
        if self._closed:
            raise RedisError('Pub/sub instance closed')
        if channels:
            if isinstance(channels, (unicode, str, bytes)):
                channels = [channels]
            channels = [self._encoder(x) for x in channels]
        if patterns:
            if isinstance(patterns, (unicode, str, bytes)):
                patterns = [patterns]
            patterns = [self._encoder(x) for x in patterns]
        cmd(self, channels, patterns)

    def _add_message(self, msg):
        self._messages.append(msg)
