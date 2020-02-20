from asyncio import Lock, Event, open_connection, open_unix_connection, get_running_loop
import socket
import sys
from collections import deque
from hashlib import sha1
# binascii requires python to be compiled with zlib ?
from binascii import crc_hqx

import hiredis


# TODO are asyncio locks reantrent ?


# Exceptions
# An error response from the redis server
class RedisReplyError(Exception):
    pass


# An error from this library
class RedisError(Exception):
    pass


def encode_number(number):
    return str(number).encode()


connect_errors = (ConnectionError, socket.error, socket.timeout)


# TODO (question) Am I missing an exception type here ?
socket_errors = (IOError, OSError, socket.error, socket.timeout)


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
        elif isinstance(inp, str):
            return inp.encode(encoding, errors)
        elif isinstance(inp, bool):
            raise ValueError('Invalid input for encoding')
        elif isinstance(inp, int):
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


async def async_with_timeout(fut, timeout=None):
    if timeout is None:
        return await fut
    else:
        return await asyncio.wait_for(fut, timeout=timeout)


class Connection(object):
    # TODO (async) update all __slots__ in all places
    __slots__ = 'reader', 'writer', 'buffersize', 'name', 'commands', 'closed', 'read_lock', 'send_lock', 'chunk_send_size', 'buffer', 'parser', 'lastdatabase', 'select', 'thread_event', 'thread'

    @classmethod
    async def create(cls, endpoint, config):
        connectionhandler = config.get('connectionhandler', cls)
        connection = connectionhandler()
        await connection._init(endpoint, config)
        return connection

    async def _init(self, endpoint, config):
        connecttimeout = config.get('connecttimeout', socket.getdefaulttimeout())
        connectretry = config.get('connectretry', 0) + 1
        sockettimeout = config.get('sockettimeout', socket.getdefaulttimeout())
        buffersize = config.get('recvbuffersize', 16384)
        tcpkeepalive = config.get('tcpkeepalive', 300)
        tcpnodelay = config.get('tcpnodelay', False)
        while connectretry:
            try:
                if isinstance(endpoint, str):
                    # TODO (async) fix
                    raise NotImplementedError()
                    #sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                    #sock.settimeout(connecttimeout)
                    #sock.connect(endpoint)
                    #sock.settimeout(sockettimeout)
                    #self.socket = sock
                elif isinstance(endpoint, tuple):
                    self.reader, self.writer = await async_with_timeout(open_connection(host=endpoint[0], port=endpoint[1], limit=buffersize), connecttimeout)
                    self.buffersize = buffersize
                    # TODO (async) setup timeout
                else:
                    raise RedisError('Invalid endpoint')
                break
            except connect_errors as e:
                connectretry -= 1
                if not connectretry:
                    raise RedisError('Connection failed: %r' % e)
        # needed for cluster support
        self.name = self.writer.get_extra_info('peername')
        #self.name = self.socket.getpeername()
        # TODO (async) fix
        #if self.socket.family == socket.AF_INET6:
            #self.name = self.name[:2]
        # TCP connection settings
        # TODO (async) fix settings
        if False: #isinstance(self.socket.getsockname(), tuple):
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
        #self.buffer = bytearray(buffersize)
        self.parser = hiredis_parser()
        self.parser.send(None)
        self.lastdatabase = 0
        # TODO (async) implment
        self.thread_event = Event()
        self.thread = get_running_loop().call_soon(self.loop)
        #self.thread_event = None
        #if with_thread and thread:
            #self.thread_event = Event()
            #self.thread = thread(self.loop)
        # Password must be bytes or utf-8 encoded string
        password = config.get('password')
        if password is not None:
            cmd = Command((b'AUTH', password))
            try:
                await self.send(cmd)
                await cmd()
            except Exception:
                await self.close()
                raise

    # This code should be exception safe
    async def loop(self):
        while True:
            # TODO is this meh ?
            await self.thread_event.wait()
            self.thread_event.clear()
            if self.closed:
                return
            while self.resolve():
                pass

    # Don't accept any new commands, but the read stream might still be alive
    def close_write(self):
        self.closed = True

    async def close(self):
        self.closed = True
        try:
            self.writer.close()
            await self.writer.wait_closed()
        except Exception:
            pass
        try:
            self.reader.close()
            await self.reader.wait_close()
        except Exception:
            pass
        finally:
            self.writer = None
            self.reader = None
            if self.commands:
                # Hmmm... I think there is no threading issue here with regards to other uses of read/send locks in the code ?
                async with self.read_lock:
                    async with self.send_lock:
                        for command in self.commands:
                            # This are already sent and we don't know if they happened or not.
                            await command.set_result(RedisError('Connection closed'), dont_retry=True)
                        self.commands = []
            # TODO (async)
            #if self.thread_event:
                #self.thread_event.set()

    # TODO We dont set TCP NODELAY to give it a chance to coalese multiple sends together, another option might be to queue them together here
    async def send(self, cmd):
        try:
            async with self.send_lock:
                db_number = cmd.get_number()
                if db_number is not None and db_number != self.lastdatabase:
                    select_cmd = Command((b'SELECT', db_number))
                    for x in select_cmd.stream(self.chunk_send_size):
                        self.writer.write(x)
                        # TODO (async) should I drain ?
                    self.commands.append(select_cmd)
                    self.lastdatabase = db_number
                if cmd._asking:
                    asking_cmd = Command((b'ASKING', ))
                    for x in asking_cmd.stream(self.chunk_send_size):
                        self.writer.write(x)
                    cmd._asking = False
                    self.commands.append(asking_cmd)
                # We send before we append to commands list because another thread might do resolve, and this will race
                for x in cmd.stream(self.chunk_send_size):
                    self.writer.write(x)
                    await self.writer.drain()
                # pub/sub commands do not expect a result
                if cmd.set_resolver(self):
                    self.commands.append(cmd)
                # TODO (async)
                # if self.thread_event:
                    #self.thread_event.set()
        # We should only mark the socket for closing when it's dead (and not for example in decoder exception)
        except socket_errors as e:
            self.close_write()
            await cmd.set_result(e)
        except Exception as e:
            await cmd.set_result(e)

    async def resolve(self, cmd=None):
        try:
            async with self.read_lock:
                # Double lock safety check
                if cmd and cmd._got_result:
                    return False
                cmd = self.commands.popleft()
                num_results = cmd.how_many_results()
                results = [await self.recv() for _ in range(num_results)]
            await cmd.set_result(results)
            return True
        except IndexError:
            pass
        except Exception as e:
            await self.close()
            # We should not retry if it/s I/O error (it might have already been executed)
            await cmd.set_result(e, dont_retry=True)
            # We don't raise here, because it makes no sense ?
        return False

    async def recv(self, allow_empty=False):
        try:
            res = self.parser.send(None)
            while res is False:
                if allow_empty:
                    return res
                # TODO (async)
                #length = self.socket.recv_into(self.buffer)
                #if length == 0:
                    #raise RedisError('Connection closed')
                #res = self.parser.send((self.buffer, 0, length))
                buffer = await self.reader.read(self.buffersize)
                if not buffer:
                    raise RedisError('Connection closed')
                res = self.parser.send((buffer, 0, len(buffer)))
            return res
        except Exception:
            await self.close()
            raise

    # TODO (async) fix
    def wait(self, timeout):
        res = select([self.socket], [], [], timeout)
        if not res[0]:
            return False
        return True


class Command(object):
    __slots__ = '_data', '_database', '_resolver', '_encoder', '_decoder', '_throw', '_got_result', '_result', '_retries', '_enqueue', '_server', '_asking'

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

    async def set_result(self, result, dont_retry=False):
        try:
            if not isinstance(result, Exception):
                result = result[0]
            if self._database and not dont_retry and self._retries and isinstance(result, Exception):
                self._retries -= 1
                # We will only retry if it's a network I/O or some redis logic
                if isinstance(result, socket_errors):
                    await self._database._multiplexer._send_command(self)
                    return
                elif isinstance(result, RedisReplyError):
                    # hiredis exceptions are already encoded....
                    if result.args[0].startswith('NOSCRIPT'):
                        sha = self._data[1]
                        script = self._database._scripts_sha.get(sha)
                        if script:
                            self._data = (b'EVAL', script) + self._data[2:]
                            await self._database._multiplexer._send_command(self)
                            return
                    elif result.args[0].startswith('MOVED'):
                        _, hashslot, addr = result.args[0].split(' ')
                        hashslot = int(hashslot)
                        addr = addr.rsplit(':', 1)
                        addr = (addr[0], int(addr[1]))
                        await self._database._multiplexer._update_slots(moved_hint=(hashslot, addr))
                        self._server = addr
                        await self._database._multiplexer._send_command(self)
                        return
                    elif result.args[0].startswith('ASKING'):
                        _, hashslot, addr = result.args[0].split(' ')
                        hashslot = int(hashslot)
                        addr = addr.rsplit(':', 1)
                        addr = (addr[0], int(addr[1]))
                        self._asking = True
                        self._server = addr
                        await self._database._multiplexer._send_command(self)
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

    async def __call__(self):
        if not self._got_result and not self._resolver:
            raise RedisError('Command is not finalized yet')
        # This is a loop because of multi threading.
        while not self._got_result:
            await self._resolver.resolve(self)
        if self._throw and isinstance(self._result, Exception):
            raise self._result
        return self._result


class Multiplexer(object):
    __slots__ = '_endpoints', '_configuration', '_connections', '_last_connection', '_tryindex', '_scripts', '_scripts_sha', '_pubsub', '_lock', '_clustered', '_command_cache', '_already_asking_for_slots', '_slots'

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

    # TODO have a closed flag, and stop requesting new connections?
    async def close(self):
        async with self._lock:
            try:
                for connection in self._connections.values():
                    await connection.close()
            except Exception:
                pass
            self._connections = {}
            self._last_connection = None

    # TODO (question) is this sane or we should not support this (explicitly call close?)
    #async def __del__(self):
        #await self.close()

    def database(self, number=0, encoder=None, decoder=None, retries=3, server=None):
        if number != 0 and self._clustered and server is None:
            raise RedisError('Redis cluster has no database selection support')
        return Database(self, number, encoder, decoder, retries, server)

    # TODO support optional server instance for pub/sub ? (for keyspace notification at least)
    # TODO (async)
    def pubsub(self, encoder=None, decoder=None):
        if self._pubsub is None:
            with self._lock:
                if self._pubsub is None:
                    self._pubsub = PubSub(self)
        return PubSubInstance(self._pubsub, encoder, decoder)

    async def endpoints(self):
        if self._clustered is None:
            await self._get_connection()
        # TODO is this atomic ?
        if self._clustered:
            return [x[1] for x in list(self._slots)]
        else:
            return list(self._connections.keys())

    async def run_commandreply_on_all_masters(self, *args, **kwargs):
        if self._clustered is False:
            raise RedisError('This command only runs on cluster mode')
        res = {}
        for endpoint in self.endpoints():
            res[endpoint] = await self.database(server=endpoint).commandreply(*args, **kwargs)
        return res

    async def _get_connection(self, hashslot=None):
        if not hashslot:
            if self._last_connection is None or self._last_connection.closed:
                async with self._lock:
                    if self._last_connection is None or self._last_connection.closed:
                        # TODO should we enumerate self._uri or self.connections (which is not populated, maybe populate at start and avoid cluster problems)
                        for num, addr in enumerate(list(self._endpoints)):
                            self._tryindex = num
                            try:
                                conn = self._connections.get(addr)
                                if not conn or conn.closed:
                                    conn = await Connection.create(addr, self._configuration)
                                    # TODO is the name correct here? (in ipv4 yes, in ipv6 no ?)
                                    self._last_connection = self._connections[conn.name] = conn
                                    self._last_connection = conn
                                    # TODO reset each time?
                                    if self._clustered is None:
                                        await self._update_slots(with_connection=conn)
                                    return conn
                            except Exception as e:
                                exp = e
                                try:
                                    await conn.close()
                                except:
                                    pass
                                self._connections.pop(addr, None)
                        self._last_connection = None
                        raise exp
            return self._last_connection
        else:
            if not self._slots:
                await self._update_slots()
            for slot in self._slots:
                if hashslot > slot[0]:
                    continue
                break
            addr = slot[1]
            try:
                conn = self._connections.get(addr)
                if not conn or conn.closed:
                    #TODO lock per conn
                    async with self._lock:
                        conn = self._connections.get(addr)
                        if not conn or conn.closed:
                            conn = await Connection.create(addr, self._configuration)
                            self._connections[addr] = conn
                return conn
            # TODO (misc) should we try getting another connection here ?
            except Exception:
                await self._update_slots()
                raise

    # TODO check if closed and dont allow
    async def _send_command(self, cmd):
        server = cmd._server
        if not server:
            if isinstance(cmd, MultiCommand) and self._clustered is not False:
                for acmd in cmd._cmds[1:-1]:
                    hashslot = await self._get_command_from_cache(acmd)
                    if hashslot is not None:
                        break
            else:
                hashslot = await self._get_command_from_cache(cmd)
            connection = await self._get_connection(hashslot)
        else:
            try:
                conn = self._connections.get(server)
                if not conn or conn.closed:
                    #TODO lock per conn
                    async with self._lock:
                        conn = self._connections.get(server)
                        if not conn or conn.closed:
                            conn = await Connection.create(server, self._configuration)
                            self._connections[server] = conn
                connection = conn
            # TODO (misc) should we try getting another connection here ?
            except Exception:
                await self._update_slots()
                raise
        await connection.send(cmd)

    async def _update_slots(self, moved_hint=None, with_connection=None):
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
            connection = with_connection or await self._get_connection()
            await connection.send(cmd)
            try:
                slots = await cmd()
                self._clustered = True
            except RedisReplyError:
                slots = []
                self._clustered = False
            slots.sort(key=lambda x: x[0])
            slots = [(x[1], (x[2][0].decode(), x[2][1])) for x in slots]
            # release hosts not here from previous connections
            remove_connections = set(self._connections) - set([x[1] for x in slots])
            for entry in remove_connections:
                try:
                    connections = self._connections.pop(entry)
                    await connections.close()
                except Exception:
                    pass
            self._slots = slots
        finally:
            self._already_asking_for_slots = False

    # It is unreasonable to expect the user to provide us with the key index for each command so we ask the server and cache it
    # TODO what to do in MULTI case ? maybe this should be in Command itself
    async def _get_command_from_cache(self, cmd):
        if self._clustered is False:
            return None
        command_name = cmd.get_index_as_bytes(0)
        keyindex = self._command_cache.get(command_name)
        if keyindex is None:
            # Requires redis server 2.8.13 or above
            # allow retries? might be infinite loop...
            info_cmd = Command((b'COMMAND', b'INFO', command_name))
            # If connection is dead, then the initiator command should retry
            await (await self._get_connection()).send(info_cmd)
            # We do this check here, since _get_connection first try will trigger a clustred check and we need to abort if it's not (to not trigger the send_command's hashslot)
            if self._clustered is False:
                return None
            cmdinfo = (await info_cmd())[0]
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

    async def command(self, *args, **kwargs):
        cmd = parse_command(self, *args, **kwargs)
        await self._multiplexer._send_command(cmd)
        return cmd

    async def commandreply(self, *args, **kwargs):
        return await self.command(*args, **kwargs)()

    # TODO (async) implment
    def multi(self, retries=None):
        return MultiCommand(self, retries if retries is not None else self._retries, server=self._server)


# TODO maybe split the user facing API from the internal Command one (atleast mark it as _)
# To know the result of a multi command simply resolve any command inside
class MultiCommand(object):
    __slots__ = '_database', '_retries', '_server', '_cmds', '_done', '_asking'

    def __init__(self, database, retries, server):
        self._database = database
        self._retries = retries
        self._server = server
        self._cmds = []
        self._done = False
        self._asking = False

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
            if isinstance(channels, (str, bytes)):
                channels = [channels]
            channels = [self._encoder(x) for x in channels]
        if patterns:
            if isinstance(patterns, (str, bytes)):
                patterns = [patterns]
            patterns = [self._encoder(x) for x in patterns]
        cmd(self, channels, patterns)

    def _add_message(self, msg):
        self._messages.append(msg)
