from asyncio import Semaphore, wait_for, Lock
from collections import deque

from . import Multiplexer, RedisError
from .justredis import Connection


# TODO add minconnections ? remove old expired connections ?
# TODO is the self._limit semaphore being counted correctly ?
class ConnectionPool:
    def __init__(self, addr, configuration, maxconnections):
        self._addr = addr
        self._configuration = configuration
        if not maxconnections:
            raise Exception('Please set a maximum limit of connections in the pool')
        self._limit = Semaphore(maxconnections)
        self._available = deque()
        self._inuse = set()
        self._fast = None
        self._lock = Lock()

    async def take(self, is_slow, timeout=None):
        if not is_slow:
            if self._fast is None or self._fast.closed:
                async with self._lock:
                    if self._fast is None or self._fast.closed:
                        self._fast = await Connection.create(self._addr, self._configuration)
            return self._fast
        else:
            try:
                while True:
                    conn = self._available.popleft()
                    if not conn.closed:
                        break
                    self._limit.release()
            except IndexError:
                if timeout is None:
                    await self._limit.acquire()
                else:
                    await wait_for(self._limit.acquire(), timeout)
                try:
                    conn = await Connection.create(self._addr, self._configuration)
                    conn.set_release_cb(self.release)
                except:
                    self._limit.release()
                    raise
            self._inuse.add(conn)
            return conn

    def release(self, conn):
        # This is also a protection against double .release call on a single .take call
        self._inuse.remove(conn)
        if not conn.closed:
            self._available.append(conn)
        else:
            self._limit.release()

    async def aclose(self):
        if self._fast:
            await self._fast.aclose()
            self._fast = None
        if self._available is not None and self._inuse is not None:
            tmp = list(self._available) + list(self._inuse)
            for conn in tmp:
                await conn.aclose()
            self._addr = None
            self._configuration = None
            self._limit = None
            self._available = None
            self._inuse = None
            self._lock = None


# This commands are handled by a higher level API and should not be called directly
not_allowed_commands = set((b'WATCH', b'MULTI', b'EXEC', b'DISCARD', b'AUTH', b'SELECT', b'SUBSCRIBE', b'PSUBSCRIBE', b'UNSUBSCRIBE', b'PUNSUBSCRIBE', b'MONITOR'))


class MultiplexerPool(Multiplexer):
    def __init__(self, configuration={}):
        super(MultiplexerPool, self).__init__(configuration)
        self._maxconnections = configuration.get('maxconnections', 10)
        self._not_allowed_commands = not_allowed_commands

    # TODO this should be refactored into the main Multiplexer._get_connection
    async def _get_connection(self, addr=None, is_slow=False):
        if addr:
            return await self._connections.setdefault(addr, ConnectionPool(addr, self._configuration, self._maxconnections)).take(is_slow)
        else:
            # TODO should we round-robin ?
            if self._last_connection is None:
                # TODO should we enumerate self._endpoints or self.connections (which is not populated, maybe populate at start and avoid cluster problems)
                # TODO should we keep an index of failed indexes, and start to resume from the last failed one the next time (_tryindex) ?
                if not self._endpoints:
                    raise RedisError('endpoints list is empty')
                for addr in list(self._endpoints):
                    try:
                        pool = self._connections.get(addr)
                        if not pool:
                            # TODO is the name correct here? (in ipv4 yes, in ipv6 no ?)
                            self._last_connection = pool = self._connections[addr] = ConnectionPool(addr, self._configuration, self._maxconnections)
                            # TODO reset each time? (i.e. if we move from a clustered server to a non clustered one)
                            if self._clustered is None:
                                await self._update_slots(with_connection=await pool.take(is_slow))
                        return await pool.take(is_slow)
                    except Exception as e:
                        exp = e
                        if pool:
                            await pool.aclose()
                        self._connections.pop(addr, None)
                self._last_connection = None
                raise exp
            return await self._last_connection.take(is_slow)
