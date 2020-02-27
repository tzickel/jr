# What?
An asynchronous redis client library for Python 3.6+

# Why?
* All commands are pipelined by default
* No connection pool, a single connection per redis server even between different execution contexts (extra one if using pub/sub)
* Optional per connection or per command encoding / decoding / pipelining

# Also supports
* Redis Cluster
* Pub/Sub
* Transparent script caching
* Hiredis parser (required)
* Testing with private redis server and cluster

# Inherit limitations
* Cannot issue blocking commands (such as BLPOP)
* Cannot issue transaction commands with WATCH (but MULTI and EXEC can be used)

This issues can be solved by writing a connection pool ontop of existing API.

# Roadmap
- [ ] API Finalization
- [ ] Finish little holes in currently supported commands
- [ ] Network I/O failure and concurrency tests
- [ ] Choose license
- [ ] Higher level API ?

# Installing
For now you can install this via this github repository by pip installing or adding to your requirements.txt file:

```
git+git://github.com/tzickel/jr@master#egg=justredis
```

Replace master with the specific branch or version tag you want.

# Examples
```python
from justredis import Multiplexer, utf8_bytes_as_strings
import asyncio


async def main():
    # Connect to the default redis port on localhost
    async with Multiplexer() as redis:
        # Send commands to database #0 (and use by default bytes as utf8 strings decoder)
        db = redis.database(decoder=utf8_bytes_as_strings)
        # Shortcut so you don't have to type long words each time
        c = db.command
        cr = db.commandreply
        # Send an pipelined SET request where you don't care about the result (You don't have to use bytes notation or caps)
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

# API
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
    multi(retries=None)
      command(*args, encoder=utf8_encode, decoder=None, throw=True)
        async __call__()
      async execute()
      async discard()
      async __aenter__()
      async __aexit__()
  pubsub(encoder=utf8_encode, decoder=None)
    async add(channels=None, patterns=None)
    async remove(channels=None, patterns=None)
    async message(timeout=None)
    async ping(message=None)
  async endpoints()
  async run_commandreply_on_all_masters(*args, encoder=utf8_encode, decoder=None)
```

Notice that .command in multi is not awaitable

Where configuration is a list of endpoints or a dictionary that can contain the following keys:

* endpoints - The connection endpoints
* password - The server password
* connecttimeout - Set connect timeout in seconds
* connectretry - Number of connection attempts before giving up
* sockettimeout - Set socket timeout in seconds
* recvbuffersize - Socket receive buffer size in bytes (Default 64K)
* tcpkeepalive - Enable / Disable (Default) TCP Keep alive in seconds
* tcpnodelay - Enable (Default) / Disable TCP no delay
* connectionhandler - Use a custom Connection class handler

# Partially inspired by
The .NET Redis client package [ServiceStack.Redis](https://stackexchange.github.io/StackExchange.Redis/)
