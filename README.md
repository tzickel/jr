## What?
An asynchronous redis client library for Python 3.6+

Please note that this project is currently alpha quality and the API is not finalized. Please provide feedback if you think the API is convenient enough or not.

## Why?
### All commands are pipelined by default

Since most Redis server commands are intended to be run for a short time on the server, sometimes you pay more time on network latency than execution time. By splitting the sending and receiving parts into seperate coroutines all commands are sent seperately from waiting for their response.

### No connection pool, one socket connection per instance (another extra for pub/sub)

Building on the first point, you can multiplex multiple coroutinues that want to communicate with a Redis server together. Each multiplexer maintains a single socket per server.

This poses some restrictions and thus blocking commands (such as BLPOP) or statefull commands (such as WATCH, but not regular MULTI and EXEC transactions) cannot be used.

A connection pool can be built on top of the multiplexer which will manage multiple connections and allow for such commands.

### Transparent Redis Cluster support

The library handles behind the scenes all the cluster managment. You use the regular non-cluster API and it figures it all out. There are special commands such as running a command on all masters, etc...

Currently the library talks to the current masters of the cluster.

### Other features including

1. Sane Publish / Subcscribe API
2. Transparent script caching
3. Optional per connection or per command encoding / decoding / pipelining
4. Automated testing for both single and cluster
5. Hiredis parser (required)

## Roadmap
- [ ] Choose license
- [ ] API Finalization
- [ ] Remove all TODO incode
- [ ] More test coverage and test out network I/O failure and concurrency


## Installing
For now you can install this via this github repository by pip installing or adding to your requirements.txt file:

```
git+git://github.com/tzickel/justredis@master#egg=justredis
```

Replace master with the specific branch or version tag you want.

## Examples
```python
from justredis import Multiplexer, utf8_bytes_as_strings
import asyncio


async def main():
    # Connect to the default redis port on localhost
    async with Multiplexer() as redis:
        # Send commands to database #0 (and decode the results as utf-8 strings instead of bytes)
        db = redis.database(decoder=utf8_bytes_as_strings)
        # Shortcut so you don't have to type long words each time
        c = db.command
        cr = db.commandreply
        # Send an pipelined SET request where you don't care about the result (You don't have to use bytes notation or caps for the command name)
        await c(b'SET', 'Hello', 'World!')
        # Send a pipelined GET request and resolve it immediately
        print('Hello, %s' % await cr(b'GET', 'Hello'))
        # You can even send both commands together atomically (so if the first fails the second won't run)
        async with db.multi() as m:
            m.command(b'SET', 'Hello', 'World!')
            hello = m.command(b'GET', 'Hello')
        print('Atomic Hello, %s' % await hello())


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.close()
```

You can check the [tests](tests/test.py) for some more examples such as pub/sub usage.

## API
This is all the API in a nutshell:

```python
Multiplexer(configuration=None)
  async __aenter__()
  async __aexit__()
  async aclose()
  database(number=0, encoder=utf8_encode, decoder=None)
    async command(*args, encoder=utf8_encode, decoder=None, throw=True)
      async __call__()
    async commandreply(*args, encoder=utf8_encode, decoder=None, throw=True)
    multi()
      async __aenter__()
      async __aexit__()
      # Notice that this command is not awaitable
      command(*args, encoder=utf8_encode, decoder=None, throw=True)
        # But the result is
        async __call__()
      # This command will be automatically called when leaving the context manager
      async execute()
      # This command will be automatically called when leaving the context manager on exception (or can be called explicitly to abort)
      async discard()
  pubsub(encoder=utf8_encode, decoder=None)
    async add(channels=None, patterns=None)
    async remove(channels=None, patterns=None)
    async message(timeout=None)
    async ping(message=None)
  async endpoints()
  async run_commandreply_on_all_masters(*args, encoder=utf8_encode, decoder=None)
```

The error model is 2 main Exceptions:

```python
# An error response from the redis server for a sent command
RedisReplyError(Exception)

# An error from this library (usually means your command might have not reached the server)
RedisError(Exception)
```

Multiplexer configuration is a list of endpoints or a dictionary that can contain the following keys:

* endpoints - The connection endpoints (a list where each element is a string for unix domain or (host, port) tuple for tcp)
* password - The server password
* connecttimeout - Set connect timeout in seconds
* connectretry - Number of connection attempts before giving up
* sockettimeout - Set socket timeout in seconds
* recvbuffersize - Socket receive buffer size in bytes (Default 64K)
* tcpkeepalive - Enable / Disable (Default) TCP Keep alive in seconds
* tcpnodelay - Enable (Default) / Disable TCP no delay
* connectionhandler - Use a custom Connection class handler

## Partially inspired by
The .NET Redis client package [ServiceStack.Redis](https://stackexchange.github.io/StackExchange.Redis/)
