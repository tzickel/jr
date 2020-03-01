from asyncio import Semaphore, wait_for
from collections import deque


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

    async def take(self, timeout=None):
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
            conn = self._factory(self.release)
        self._inuse.append(conn)
        return conn

    async def release(self, conn):
        # TODO is this the best option ?
        self._inuse.remove(conn)
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
