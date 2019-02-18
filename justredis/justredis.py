import sys
import socket
from threading import Lock
from collections import deque
from hashlib import sha1

import hiredis

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


not_allowed_commands = (b'WATCH', b'BLPOP', b'MULTI', b'EXEC', b'DISCARD', b'BRPOP', b'AUTH', b'SELECT')


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


def chunk_encoded_command(cmd, chuck_size):
    data = []
    data_len = 0
    for x in cmd.encode():
        chunk_len = len(x)
        if data_len > chuck_size or chunk_len > chuck_size:
            yield b''.join(data)
            if chunk_len > chuck_size:
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


def chunk_encoded_commands(cmds, chuck_size):
    data = []
    data_len = 0
    cmd_index = 0
    for cmd in cmds:
        for x in cmd.encode():
            chunk_len = len(x)
            if data_len > chuck_size or chunk_len > chuck_size:
                yield b''.join(data)
                if chunk_len > chuck_size:
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


# TODO better handling for unix domain, and default ports in tuple / list
def parse_uri(uri):
    if isinstance(uri, Connection):
        return uri
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
    # First argument should always be utf-8 encoded english?
    cmd = args[0]
    cmd = (cmd if isinstance(cmd, bytes) else cmd.encode()).upper()
    if cmd in not_allowed_commands:
        raise RedisError('Command is not allowed')
    encoder = source._encoder
    decoder = source._decoder
    retries = source._retries
    throw = True
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
    @classmethod
    def create(cls, endpoint, config):
        connectionhandler = config.get('connectionhandler', cls)
        return connectionhandler(endpoint, config)

    def __init__(self, endpoint, config, socket_class=socket, lock_class=Lock):
        connecttimeout = config.get('connecttimeout', socket.getdefaulttimeout())
        connectretry = config.get('connectretry', 0) + 1
        sockettimeout = config.get('sockettimeout', socket.getdefaulttimeout())
        buffersize = config.get('recvbuffersize', 16384)
        tcpkeepalive = config.get('tcpkeepalive', 300)
        while connectretry:
            try:
                if isinstance(endpoint, str):
                    sock = socket_class.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                    sock.settimeout(connecttimeout)
                    sock.connect(endpoint)
                    sock.settimeout(sockettimeout)
                    self.socket = sock
                elif isinstance(endpoint, tuple):
                    self.socket = socket_class.create_connection(endpoint, timeout=connecttimeout)
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
        if tcpkeepalive:
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            # TODO support other platforms (atleast windows)
            if platform == 'linux':
                self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, tcpkeepalive)
                self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, tcpkeepalive // 3)
                self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3)
        self.commands = deque()
        self.closed = False
        self.read_lock = lock_class()
        self.send_lock = lock_class()
        self.chunk_send_size = 6000
        self.buffer = bytearray(buffersize)
        self.parser = hiredis_parser()
        self.parser.send(None)
        self.lastdatabase = 0
        password = config.get('password')
        if password is not None:
            cmd = Command((b'AUTH', password))
            try:
                self.send(cmd)
                cmd()
            except:
                self.close()
                raise

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
                # TODO (thread) threading issue here, we need to check in other places if self.closed and throw exception (check in locks which use this)
                with self.read_lock:
                    with self.send_lock:
                        for command in self.commands:
                            # This are already sent and we don't know if they happened or not.
                            command.set_result(RedisError('Connection closed'), dont_retry=True)
                        self.commands = []

    # TODO (performance) we can actually try to coalese multiple sends here, but let's give the tcp stack an option to do this (because of DELAY)
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
                # TODO (pubsub) dont append if pubsub cmd._enqueue (not implemented yet)
                self.commands.append(cmd)
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


class Command(object):
    __slots__ = '_data', '_database', '_resolver', '_encoder', '_decoder', '_throw', '_got_result', '_result', '_retries'

    def __init__(self, data, database=None, encoder=None, decoder=None, throw=True, retries=3):
        self._data = data
        self._database = database
        self._resolver = None
        self._encoder = encoder or utf8_encode
        self._decoder = decoder
        self._throw = throw
        self._got_result = False
        self._result = None
        self._retries = retries

    def get_number(self):
        return self._database._number if self._database else None
    
    def how_many_results(self):
        return 1

    def stream(self, chunk_size):
        return chunk_encoded_command(self, chunk_size)

    # This causes encoding errors to be detected at socket I/O level (which can close it), but is faster otherwise
    def encode(self):
        return encode_command(self._data, self._encoder)

    def set_resolver(self, connection):
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


# TODO add pubsub !!!
# TODO thread safety !!!
class Multiplexer(object):
    __slots__ = '_endpoints', '_configuration', '_connection', '_scripts', '_scripts_sha', '_pubsub'

    def __init__(self, configuration=None):
        self._endpoints, self._configuration = parse_uri(configuration)
        self._connection = None
        # The reason we do double dictionary here is for faster lookup in case of lookup failure and multi threading protection
        self._scripts = {}
        self._scripts_sha = {}
        self._pubsub = None

    def close(self):
        if self._connection:
            self._connection.close()
            self._connection = None

    # TODO (misc) is this sane or we should not support this (explicitly call close?)
    def __del__(self):
        self.close()

    def database(self, number=0, encoder=None, decoder=None, retries=3):
        return Database(self, number, encoder, decoder, retries)

    def pubsub(self):
        if self._pubsub:
            return self._pubsub
        return PubSub(self)

    def _send_command(self, cmd):
        if self._connection is None or self._connection.closed:
            self._connection = Connection.create(self._endpoints[0], self._configuration)
        self._connection.send(cmd)


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

    def multi(self, retries=None):
        return MultiCommand(self, retries if retries is not None else self._retries)


class MultiCommand(object):
    __slots__ = '_database', '_cmds', '_done', '_retries'

    def __init__(self, database, retries):
        self._database = database
        self._cmds = []
        self._done = False
        self._retries = retries

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


class PubSub(object):
    def __init__(self, multiplexer):
        self._multiplexer = multiplexer
        self._channels = []
        self._patterns = []

    def subscribe(self, *channels):
        pass
    
    def unsubscribe(self, *channels):
        pass
    
    def psubscribe(self, *patterns):
        pass
    
    def punsubscribe(self, *patterns):
        pass
    
    def ping(self, msg):
        pass

    def close(self):
        pass

    def message(self, timeout=None):
        pass
