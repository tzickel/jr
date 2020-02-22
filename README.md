# What?
An asynchronous redis client library for Python 3.7+

# Why?
* All commands are pipelined (asynchronous)
* No connection pool, a single connection per redis server even between different execution contexts (extra one if using pub/sub)
* Optional per command encoding / decoding / retries

# Also supports
* Redis Cluster
* Pub/Sub
* Transparent script caching
* Retry support only when it's safe
* Hiredis parser
* Testing with private redis server and cluster

# Inherit limitations
* Cannot issue blocking commands (such as BLPOP)
* Cannot issue transaction commands with WATCH (but MULTI and EXEC can be used)

# Roadmap
- [ ] API Finalization
- [ ] Finish little holes in currently supported commands
- [ ] Network I/O failure and concurrency tests
- [ ] Choose license
- [ ] Higher level API ?

# Installing
For now you can install this via this github repository by pip installing or adding to your requirements.txt file:

```
https://github.com/tzickel/justredis/archive/master.zip
```

Replace master with the specific branch or version tag you want.

# Examples
```python
from justredis import MultiplexerAsync, utf8_bytes_as_strings
import asyncio


async def main():
    # Connect to the default redis port on localhost
    redis = MultiplexerAsync()
    # Send commands to database #0 (and use by default bytes as utf8 strings decoder)
    db = redis.database(decoder=utf8_bytes_as_strings)
    # Shortcut so you don't have to type long words each time
    c = db.command
    cr = db.commandreply
    # Send an pipelined SET request where you don't care about the result (You don't have to use bytes notation or caps)
    await c(b'SET', 'Hello', 'World!')
    # Send a pipelined GET request and resolve it immediately
    print('Hello, %s' % await cr('get', 'Hello'))
    # You can even send both commands together atomically (so if the first fails the second won't run)
    async with db.multi() as m:
        m.command(b'SET', 'Hello', 'World!')
        hello = m.command('get', 'Hello')
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
  database(number=0, encoder=utf8_encode, decoder=None, retries=3, server=None)
    command(*args, encoder=utf8_encode, decoder=None, retries=3, throw=True)
      __call__()
    commandreply(*args, encoder=utf8_encode, decoder=None, retries=3, throw=True)
    multi(retries=None)
      command(*args, encoder=utf8_encode, decoder=None, retries=3, throw=True)
        __call__()
      execute()
      discard()
      __enter__()
      __exit__()
  pubsub(encoder=utf8_encode, decoder=None)
    add(channels=None, patterns=None)
    remove(channels=None, patterns=None)
    message(max_timeout=None)
    ping(message=None)
  endpoints()
  run_commandreply_on_all_masters(*args, encoder=utf8_encode, decoder=None, retries=3)
  close()
```

Where configuration is a list of endpoints or a dictionary that can contain the following keys:

* endpoints - The connection endpoints
* password - The server password
* connecttimeout - Set connect timeout in seconds
* connectretry - Number of connection attempts before giving up
* sockettimeout - Set socket timeout in seconds
* recvbuffersize - Socket receive buffer size in bytes (Default 16K)
* tcpkeepalive - Enable (Default 300 seconds) / Disable TCP Keep alive in seconds
* tcpnodelay - Enable / Disable (Default) TCP no delay
* connectionhandler - Use a custom Connection class handler

# Partially inspired by
The .NET Redis client package [ServiceStack.Redis](https://stackexchange.github.io/StackExchange.Redis/)
