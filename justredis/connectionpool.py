from asyncio import Semaphore, wait_for, Lock
from collections import deque

from . import Multiplexer, RedisError
from .justredis import Connection


# TODO remove old expire items
# TODO use a queue ?
class ConnectionPool:
    def __init__(self, factory, maxconnections):
        self._available = deque()
        self._inuse = deque()
        self._factory = factory
        if not maxconnections:
            raise Exception('Please set a maximum limit of connections in the pool')
        self._limit = Semaphore(maxconnections)
        self._fast = None
        self._lock = Lock()

    async def take(self, is_slow, timeout=None):
        if not is_slow:
            if self._fast is None or self._fast.closed:
                async with self._lock:
                    if self._fast is None or self._fast.closed:
                        # TODO don't release here
                        self._fast = await self._factory()
            return self._fast
        else:
            try:
                while True:
                    conn = self._available.popleft()
                    if not conn.closed:
                        break
            except IndexError:
                if timeout is None:
                    await self._limit.acquire()
                else:
                    wait_for(self._limit.acquire(), timeout)
                conn = await self._factory(self.release)
    #        print('put', conn)
    #        self._inuse.append(conn)
            return conn

    def release(self, conn):
        # TODO is this the best option ?
#        print('remove', conn)
#        self._inuse.remove(conn)
        if not conn.closed:
            self._available.append(conn)
        self._limit.release()

    async def aclose(self):
        if self._available is not None and self._inuse is not None:
            tmp = list(self._available) + list(self._inuse)
            for conn in tmp:
                await conn.aclose()
            self._limit = None
            self._factory = None
            self._available = None
            self._inuse = None


class MultiplexerPool(Multiplexer):
    def __init__(self, configuration=None):
        super(MultiplexerPool, self).__init__(configuration)

    def new_connection(self, addr, is_slow):
        async def factory(release_func, addr=addr, is_slow=is_slow):
            conn = await Connection.create(addr, self._configuration)
            if is_slow:
                conn.set_release_cb(release_func)
            return conn
        return ConnectionPool(factory, 10)

    async def _get_connection(self, addr=None, is_slow=False):
        if addr:
            pool = self._connections.get(addr)
            if pool is None:
                pool = self._connections[addr] = self.new_connection(addr)
            return await pool.take(is_slow)
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
                            self._last_connection = pool = self._connections[addr] = self.new_connection(addr)
                            # TODO reset each time?
                            if self._clustered is None:
                                await self._update_slots(with_connection=await pool.take(is_slow))
                        return await pool.take(is_slow)
                    except Exception as e:
                        exp = e
                        if conn:
                            await conn.aclose()
                        self._connections.pop(addr, None)
                self._last_connection = None
                raise exp
            return await self._last_connection.take(is_slow)
