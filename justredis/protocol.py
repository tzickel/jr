import asyncio
from collections import deque

import hiredis
from .exceptions import RedisError, RedisReplyError


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


def encode_command(data, encoder):
    output = [b'*%d\r\n' % len(data)]
    for arg in data:
        arg = encoder(arg)
        output.extend((b'$%d\r\n' % len(arg), arg, b'\r\n'))
    return output


try:
    from asyncio import BufferedProtocol as BaseProtocol
# Python 3.6 support
except ImportError:
    from asyncio import Protocol as BaseProtocol


class RedisProtocol(BaseProtocol):
    __slots__ = '_buffer', '_transport', '_reader', '_messages', '_wait', '_eof', '_closed', '_write_drained'

    @classmethod
    async def create_connection(cls, *args, **kwargs):
        _, protocol = await asyncio.get_event_loop().create_connection(cls, *args, **kwargs)
        return protocol

    @classmethod
    async def create_unix_connection(cls, *args, **kwargs):
        _, protocol = await asyncio.get_event_loop().create_unix_connection(cls, *args, **kwargs)
        return protocol

    def __init__(self):
        self._buffer = None
        self._transport = None
        self._reader = hiredis.Reader()
        self._messages = deque()
        self._wait = asyncio.Event()
        self._eof = False
        self._closed = False
        self._write_drained = asyncio.Event()
        self._write_drained.set()

    def connection_made(self, transport):
        self._transport = transport

    def connection_lost(self, exc):
        if exc:
            self._eof = exc
        else:
            self._eof = True
        self._wait.set()
        self._buffer = None
        self._reader = None

    def get_buffer(self, sizehint):
        if sizehint == -1:
            # TODO get this from constructor
            sizehint = 2**16
        self._buffer = bytearray(sizehint)
        return self._buffer

    def buffer_updated(self, nbytes):
        self._reader.feed(self._buffer, 0, nbytes)
        # basically this can throw an hiredis.ProtocolError but only we send it valid data, so...
        res = self._reader.gets()
        if res is not False:
            self._wait.set()
        while res is not False:
            if isinstance(res, hiredis.ReplyError):
                res = RedisReplyError(*res.args)
            self._messages.append(res)
            res = self._reader.gets()

    # Python 3.6 support
    def data_received(self, data):
        self._reader.feed(data)
        # basically this can throw an hiredis.ProtocolError but only we send it valid data, so...
        res = self._reader.gets()
        if res is not False:
            self._wait.set()
        while res is not False:
            self._messages.append(res)
            res = self._reader.gets()

    def pause_writing(self):
        self._write_drained.clear()

    def resume_writing(self):
        self._write_drained.set()

    async def drain(self):
        await self._write_drained.wait()

    def get_transport(self):
        return self._transport

    def write(self, stream):
        self._transport.writelines(stream)

    def write_raw(self, *cmd):
        self._transport.writelines(encode_command(cmd, lambda x: x.encode()))

    def close(self):
        if not self._closed:
            self._closed = True
            self._transport.write_eof()

    async def wait_closed(self):
        if not self._closed:
            raise Exception('Connection not closed yet')
        await self._wait.wait()

    async def read(self):
        if self._messages:
            return self._messages.popleft()
        if self._eof:
            if self._eof is True:
                return b''
            else:
                raise self._eof
        self._wait.clear()
        await self._wait.wait()
        if self._messages:
            return self._messages.popleft()
        if self._eof:
            if self._eof is True:
                return b''
            else:
                raise self._eof
