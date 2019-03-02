from __future__ import absolute_import
import sys
from collections import deque
from hashlib import sha1
# binascii requires python to be compiled with zlib ?
from binascii import crc_hqx

import hiredis


# Execution environment
from .environment import get_env
Lock, Event, socket, select, thread = get_env("Lock", "Event", "socket", "select", "thread")


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


# TODO refine this....
command_retry_errors = (Exception)


platform = ''
if sys.platform.startswith('linux'):
    platform = 'linux'
elif sys.platform.startswith('darwin'):
    platform = 'darwin'
elif sys.platform.startswith('win'):
    platform = 'windows'


# TODO change to set
not_allowed_commands = (b'WATCH', b'BLPOP', b'MULTI', b'EXEC', b'DISCARD', b'BRPOP', b'AUTH', b'SELECT', b'SUBSCRIBE', b'PSUBSCRIBE', b'UNSUBSCRIBE', b'PUNSUBSCRIBE')


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
# TODO (misc) this can be a generator
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
        if e > s:
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
    return Command(args, source, encoder, decoder, throw, retries)


class Connection(object):
    __slots__ = 'socket', 'name', 'commands', 'closed', 'read_lock', 'send_lock', 'chunk_send_size', 'buffer', 'parser', 'lastdatabase', 'select', 'thread_event', 'thread'
    @classmethod
    def create(cls, endpoint, config):
        connectionhandler = config.get('connectionhandler', cls)
        return connectionhandler(endpoint, config)

    def __init__(self, endpoint, config):
        connecttimeout = config.get('connecttimeout', socket.getdefaulttimeout())
        connectretry = config.get('connectretry', 0) + 1
        sockettimeout = config.get('sockettimeout', socket.getdefaulttimeout())
        buffersize = config.get('recvbuffersize', 16384)
        tcpkeepalive = config.get('tcpkeepalive', 300)
        tcpnodelay = config.get('tcpnodelay', False)
        while connectretry:
            try:
                if isinstance(endpoint, str):
                    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                    sock.settimeout(connecttimeout)
                    sock.connect(endpoint)
                    sock.settimeout(sockettimeout)
                    self.socket = sock
                elif isinstance(endpoint, tuple):
                    self.socket = socket.create_connection(endpoint, timeout=connecttimeout)
                    self.socket.settimeout(sockettimeout)
                else:
                    raise RedisError('Invalid endpoint')
                break
            except connect_errors as e:
                connectretry -= 1
                if not connectretry:
                    raise RedisError('Connection failed: %r' % e)
        # needed for cluster support
        self.name = self.socket.getpeername()
        if isinstance(self.socket.getsockname(), tuple):
            if tcpnodelay:
                self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            if tcpkeepalive:
                self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                if platform == 'linux':
                    self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, tcpkeepalive)
                    self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, tcpkeepalive // 3)
                    self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3)
                elif platform == 'darwin':
                    self.socket.setsockopt(socket.IPPROTO_TCP, 0x10, tcpkeepalive // 3)
                elif platform == 'windows':
                    self.socket.ioctl(SIO_KEEPALIVE_VALS, (1, tcpkeepalive * 1000, tcpkeepalive // 3 * 1000))
        self.commands = deque()
        self.closed = False
        self.read_lock = Lock()
        self.send_lock = Lock()
        self.chunk_send_size = 6000
        self.buffer = bytearray(buffersize)
        self.parser = hiredis_parser()
        self.parser.send(None)
        self.lastdatabase = 0
        self.select = select
        self.thread_event = None
        if thread:
            self.thread_event = Event()
            self.thread = thread(self.loop)
        password = config.get('password')
        if password is not None:
            cmd = Command((b'AUTH', password))
            try:
                self.send(cmd)
                cmd()
            except:
                self.close()
                raise

    # This should be exception safe
    def loop(self):
        while True:
            self.thread_event.wait()
            self.thread_event.clear()
            if self.closed:
                return
            while True:
                if not self.resolve():
                    break

    # Don't accept any new commands, but the read stream might still be alive
    def close_write(self):
        self.closed = True

    def close(self):
        self.closed = True
        try:
            self.socket.close()
        except Exception:
            pass
        finally:
            self.socket = None
            if self.commands:
                # Hmmm... I think there is no threading issue here with regards to other uses of read/send locks in the code ?
                with self.read_lock:
                    with self.send_lock:
                        for command in self.commands:
                            # This are already sent and we don't know if they happened or not.
                            command.set_result(RedisError('Connection closed'), dont_retry=True)
                        self.commands = []
            if self.thread_event:
                self.thread_event.set()

    # TODO We dont set TCP NODELAY to give it a chance to coalese multiple sends together, another option might be to queue them together here
    def send(self, cmd):
        try:
            with self.send_lock:
                db_number = cmd.get_number()
                if db_number is not None and db_number != self.lastdatabase:
                    select_cmd = Command((b'SELECT', db_number))
                    for x in select_cmd.stream(self.chunk_send_size):
                        self.socket.sendall(x)
                    self.commands.append(select_cmd)
                    self.lastdatabase = db_number
                # We send before we append to commands list because another thread might do resolve, and this will race
                for x in cmd.stream(self.chunk_send_size):
                    self.socket.sendall(x)
                cmd.set_resolver(self)
                # This is for pub/sub to not expect a result
                if cmd._enqueue:
                    self.commands.append(cmd)
                if self.thread_event:
                    self.thread_event.set()
        except Exception as e:
            self.close_write()
            cmd.set_result(e)

    def resolve(self):
        try:
            with self.read_lock:
                cmd = self.commands.popleft()
                num_results = cmd.how_many_results()
                results = [self.recv() for _ in range(num_results)]
            cmd.set_result(results)
            return True
        except IndexError:
            pass
        except Exception as e:
            self.close()
            # We should not retry if it/s I/O error (handle it inside)
            cmd.set_result(e)
            # We don't raise here, because it makes no sense ?
        return False

    def recv(self, allow_empty=False):
        try:
            res = self.parser.send(None)
            while res is False:
                if allow_empty:
                    return res
                length = self.socket.recv_into(self.buffer)
                if length == 0:
                    raise RedisError('Connection closed')
                res = self.parser.send((self.buffer, 0, length))
            return res
        except Exception:
            self.close()
            raise

    def wait(self, timeout):
        # I am hoping here that select on all platforms returns a read error on closing
        res = self.select([self.socket], [], [], timeout)
        if not res[0]:
            return False
        return True


class Command(object):
    __slots__ = '_data', '_database', '_resolver', '_encoder', '_decoder', '_throw', '_got_result', '_result', '_retries', '_enqueue'

    def __init__(self, data, database=None, encoder=None, decoder=None, throw=True, retries=3, enqueue=True):
        self._data = data
        self._database = database
        self._encoder = encoder or utf8_encode
        self._decoder = decoder
        self._throw = throw
        self._retries = retries
        self._enqueue = enqueue
        self._resolver = None
        self._got_result = False
        self._result = None

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

    # This causes encoding errors to be detected at socket I/O level (which can close it), but is faster otherwise
    def encode(self):
        return encode_command(self._data, self._encoder)

    def set_resolver(self, connection):
        if self._enqueue:
            self._resolver = connection

    def set_result(self, result, dont_retry=False):
        try:
            if not isinstance(result, Exception):
                result = result[0]
            if self._database and not dont_retry and self._retries and isinstance(result, command_retry_errors):
                self._retries -= 1
                if not isinstance(result, RedisReplyError):
                    self._database._multiplexer._send_command(self)
                    return
                else:
                    # hiredis exceptions are already encoded....
                    if result.args[0].startswith('NOSCRIPT'):
                        sha = self._data[1]
                        script = self._database._scripts_sha.get(sha)
                        if script:
                            self._data = (b'EVAL', script) + self._data[2:]
                            self._database._multiplexer._send_command(self)
                            return
            # TODO (encoding) should we call decoder on Exceptions as well ?
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
            self._resolver.resolve()
        if self._throw and isinstance(self._result, Exception):
            raise self._result
        return self._result


class Multiplexer(object):
#    __slots__ = '_endpoints', '_configuration', '_connection', '_scripts', '_scripts_sha', '_pubsub', '_lock', '_clustered', '_command_cache'

    def __init__(self, configuration=None):
        self._endpoints, self._configuration = parse_uri(configuration)
        self._connections = {}
        self._last_connection = None
        self._tryindex = 0
        # The reason we do double dictionary here is for faster lookup in case of lookup failure and multi threading protection
        self._scripts = {}
        self._scripts_sha = {}
        self._pubsub = None
        self._lock = Lock()
        self._clustered = None
        self._command_cache = {}
        self._already_asking_for_slots = False
        self._slots = []

    def close(self):
        if self._connections:
            with self._lock:
                try:
                    for connection in self._connections.values():
                        connection.close()
                except Exception:
                    pass
                self._connections = {}
                self._last_connection = None

    # TODO (misc) is this sane or we should not support this (explicitly call close?)
    def __del__(self):
        self.close()

    def database(self, number=0, encoder=None, decoder=None, retries=3):
        if number != 0 and self._clustered:
            raise RedisError('Redis cluster has no database selection support')
        return Database(self, number, encoder, decoder, retries)

    def pubsub(self):
        if self._pubsub is None:
            with self._lock:
                if self._pubsub is None:
                    self._pubsub = PubSub(self)
        return PubSubInstance(self._pubsub)

    def _get_connection(self, hashslot=None):
        if not hashslot:
            if self._last_connection is None or self._last_connection.closed:
                with self._lock:
                    if self._last_connection is None or self._last_connection.closed:
                        # TODO should we enumerate self._uri or self.connections (which is not populated, maybe populate at start and avoid cluster problems)
                        for num, addr in enumerate(list(self._endpoints)):
                            self._tryindex = num
                            try:
                                conn = self._connections.get(addr)
                                if not conn or conn.closed:
                                    conn = Connection.create(addr, self._configuration)
                                    # TODO is the name correct here? (in ipv4 yes, in ipv6 no ?)
                                    self._last_connection = self._connections[conn.name] = conn
                                    self._last_connection = conn
                                    # TODO reset each time?
                                    if self._clustered is None:
                                        self._update_slots(with_connection=conn)
                                    return conn
                            except Exception:
                                try:
                                    conn.close()
                                except:
                                    pass
                                self._connections.pop(addr, None)
                        self._last_connection = None
                        raise RedisError('Could not find any connection')
            return self._last_connection
        else:
            if not self._slots:
                self._update_slots()
            for slot in self._slots:
                if hashslot > slot[0]:
                    continue
                break
            addr = slot[1]
            try:
                conn = self._connections.get(addr)
                if not conn or conn.closed:
                    #TODO lock per conn
                    with self._lock:
                        conn = self._connections.get(addr)
                        if not conn or conn.closed:
                            conn = Connection.create(addr, self._configuration)
                            self._connections[addr] = conn
                return conn
            # TODO (misc) should we try getting another connection here ?
            except Exception:
                self.update_slots()
                raise

    # TODO TRY MULTICOMMAND AS FIRST ONE?
    def _send_command(self, cmd):
        if isinstance(cmd, MultiCommand) and self._clustered is not False:
            for acmd in cmd._cmds[1:-1]:
                hashslot = self._get_command_from_cache(acmd)
                if hashslot is not None:
                    break
        else:
            hashslot = self._get_command_from_cache(cmd)
        connection = self._get_connection(hashslot)
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
                if hashslot > slot[0]:
                    continue
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
            self._slots = slots
            return
            # TODO fix this !!
            # release hosts not here from previous connections
            remove_connections = set(self.connections) - set([x[1] for x in slots])
            # TODO is this ok after connectionpool rewrite
            for entry in remove_connections:
                # the pop will make this not accept already in use sockets back
                connections = self.connections.pop(entry)
                connections.close()
                #for conn in connections:
                    #conn.close()
                #connections.clear()
            self._slots = slots
        finally:
            self._already_asking_for_slots = False

    # It is unreasonable to expect the user to provide us with the key index for each command so we ask the server and cache it
    # TODO what to do in MULTI case ? maybe this should be in Command itself
    def _get_command_from_cache(self, cmd):
        if self._clustered is False:
            return None
        command_name = cmd.get_index_as_bytes(0)
        keyindex = self._command_cache.get(command_name)
        if keyindex is None:
            # Requires redis server 2.8.13 or above
            # allow retries? might be infinite loop...
            info_cmd = Command((b'COMMAND', b'INFO', command_name))
            # If connection is dead, then the initiator command should retry
            self._get_connection().send(info_cmd)
            # We do this check here, since _get_connection first try will trigger a clustred check and we need to abort if it's not (to not trigger the send_command's hashslot)
            if self._clustered is False:
                return None
            cmdinfo = info_cmd()[0]
            # If the server does not know the command, we can't redirect it properly in cluster mode
            keyindex = cmdinfo[3] if cmdinfo else 0
            self._command_cache[command_name] = keyindex
        if keyindex == 0:
            return None
        try:
            key = cmd.get_index_as_bytes(keyindex)
        except IndexError:
            return None
        return calc_hash(key)


class Database(object):
    __slots__ = '_multiplexer', '_number', '_encoder', '_decoder', '_retries', '_scripts', '_scripts_sha'

    def __init__(self, multiplexer, number, encoder, decoder, retries):
        self._multiplexer = multiplexer
        self._number = number
        self._encoder = encoder
        self._decoder = decoder
        self._retries = retries
        self._scripts = multiplexer._scripts
        self._scripts_sha = multiplexer._scripts_sha

    def command(self, *args, **kwargs):
        cmd = parse_command(self, *args, **kwargs)
        self._multiplexer._send_command(cmd)
        return cmd

    def commandreply(self, *args, **kwargs):
        cmd = self.command(*args, **kwargs)
        return cmd()

    def multi(self, retries=None):
        return MultiCommand(self, retries if retries is not None else self._retries)


# To know the result of a multi command simply resolve any command inside
class MultiCommand(object):
    __slots__ = '_database', '_cmds', '_done', '_retries', '_enqueue'

    def __init__(self, database, retries):
        self._database = database
        self._cmds = []
        self._done = False
        self._retries = retries
        self._enqueue = True

    def stream(self, chunk_size):
        return chunk_encoded_commands(self._cmds, chunk_size)

    def set_resolver(self, connection):
        for cmd in self._cmds:
            cmd._resolver = connection

    def get_number(self):
        return self._database._number

    def set_result(self, result, dont_retry=False):
        # TODO (misc) If there is an exec error, maybe preserve the original per-cmd error as well ?
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
        
        if self._database and not dont_retry and self._retries and isinstance(exec_res, command_retry_errors):
            self._retries -= 1
            if not isinstance(exec_res, RedisReplyError):
                self._database._multiplexer._send_command(self)
                return

        for cmd in self._cmds[1:-1]:
            cmd.set_result(exec_res, dont_retry=True)
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


# TODO how to handle retries on I/O errors ?
# TODO the multiplexer .close should close this as well
# TODO encoding?
# TODO should I have a registration object ? (per object lifetime)
# This is a global instance per multiplexer
# TODO auto close by looking at the reply number of subs ?
# TODO ping ? just use keepalive ?
# TODO decoding / encoding per instance !
# TODO keys must be binaries, what does hiredis do ?

# TODO deadlock because waiting on message, and add/ing removing instances same lock !
class PubSub(object):
#    __slots__ = '_multiplexer', '_connection', '_lock', '_msg_lock', '_msg_waiting', '_registered_instances', '_registered_channels', '_registered_patterns', '_thread'

    def __init__(self, multiplexer):
        self._multiplexer = multiplexer
        self._connection = None
        self._lock = Lock()
        self._msg_lock = Lock()
        self._msg_waiting = Event()
        self._registered_instances = {}
        self._registered_channels = {}
        self._registered_patterns = {}
        if thread:
            self._thread = thread(self.loop)

    def create_connection(self):
        if self._connection is None or self._connection.closed:
            self._connection = Connection.create(self._multiplexer._endpoints[0], self._multiplexer._configuration)
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
    def message(self, instance, max_timeout=None):
        stop = False if max_timeout is None else True
        while max_timeout is None or stop:
            try:
                got_lock = self._msg_lock.acquire(False)
                if not got_lock:
                    return self._msg_waiting.wait(max_timeout)
                res = self._connection.recv(allow_empty=True)
                if res is False:
                    if self._connection.wait(max_timeout):
                        res = self._connection.recv()
                    else:
                        res = None
                if res:
                    # The reason we send subscribe messages as well, is to know when an I/O reconnection has occurred
                    if res[0] == b'message':
                        with self._lock:
                            for _instance in self._registered_channels[res[1]]:
                                _instance._add_message(res)
                                if instance == _instance:
                                    stop = True
                    elif res[0] == b'pmessage':
                        with self._lock:
                            for _instance in self._registered_patterns[res[1]]:
                                _instance._add_message(res)
                                if instance == _instance:
                                    stop = True
                    elif res[0] == b'subscribe':
                        with self._lock:
                            for _instance in self._registered_channels[res[1]]:
                                _instance._add_message(res)
                                if instance == _instance:
                                    stop = True
                    elif res[0] == b'psubscribe':
                        with self._lock:
                            for _instance in self._registered_patterns[res[1]]:
                                _instance._add_message(res)
                                if instance == _instance:
                                    stop = True
                if stop:
                    break
            finally:
                if got_lock:
                    self._msg_lock.release()
        self._msg_waiting.set()

    def loop(self):
        while True:
            self.message(None)

    # If in register / unregister there is an I/O at least it's bookkeeped first so later invocations will fix it
    def register(self, instance, channels=None, patterns=None):
        with self._lock:
            registered = self._registered_instances.setdefault(instance, [set(), set()])
            registered[0].update(channels)
            registered[1].update(patterns)
            if channels:
                for channel in channels:
                    self._registered_channels.setdefault(channel, set()).add(instance)
            if patterns:
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
            registered_channels, registered_patterns = self._registered_instances.pop(instance)
            for channel in registered_channels:
                if channels and channel not in channels:
                    continue
                registered_channel = self._registered_channels[channel]
                registered_channel.discard(instance)
                if not registered_channel:
                    channels_to_remove.append(channel)
            for pattern in registered_patterns:
                if patterns and pattern not in patterns:
                    continue
                registered_pattern = self._registered_patterns[pattern]
                registered_pattern.discard(instance)
                if not registered_pattern:
                    patterns_to_remove.append(pattern)
            if channels_to_remove:
                self._command(b'UNSUBSCRIBE', *channels_to_remove)
            if patterns_to_remove:
                self._command(b'PUNSUBSCRIBE', *patterns_to_remove)


class PubSubInstance(object):
    __slots__ = '_pubsub', '_closed', '_messages'

    def __init__(self, pubsub):
        self._pubsub = pubsub
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
            self._pubsub = None

    def _cmd(self, cmd, channels, patterns):
        if self._closed:
            raise RedisError('Pub/sub instance closed')
        if channels:
            if isinstance(channels, (unicode, str, bytes)):
                channels = [channels]
            channels = [utf8_encode(x) for x in channels]
        if patterns:
            if isinstance(patterns, (unicode, str, bytes)):
                patterns = [patterns]
            patterns = [utf8_encode(x) for x in patterns]
        cmd(self, channels, patterns)

    def add(self, channels=None, patterns=None):
        self._cmd(self._pubsub.register, channels, patterns)

    def remove(self, channels=None, patterns=None):
        self._cmd(self._pubsub.unregister, channels, patterns)

    def _add_message(self, msg):
        self._messages.append(msg)

    # Don't pass this between different invocation contexts
    def message(self, max_timeout=None):
        if self._closed:
            raise RedisError('Pub/sub instance closed')
        try:
            return self._messages.popleft()
        except IndexError:
            self._pubsub.message(self, max_timeout)
            try:
                return self._messages.popleft()
            except IndexError:
                return None
