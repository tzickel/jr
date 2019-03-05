# What?
An asynchronous redis client library for Python 2.7 and 3

# Why?
* All commands are pipelined (asynchronous)
* No connection pool, a single connection per redis instance even between different execution contexts (extra one if using pub/sub)
* Supports several execution contexts: cooperative multitasking, preemptive multithreading, gevent and pluggable support for others
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

# Installing
For now you can install this via this github repository by pip installing or adding to your requirements.txt file:

```
https://github.com/tzickel/jr/archive/master.zip
```

Replace master with the specific branch or version tag you want.

# Examples
This example uses an asynchronous syntax which is compatible with Python 2.7. You can call () on a redis command to resolve it's reply (or not if you don't care about the result). You can also use commandreply instead of command to resolve the reply directly.

```python
from justredis import Multiplexer, utf8_bytes_as_strings
# Connect to the default redis port on localhost
redis = Multiplexer()
# Send commands to database #0 (and use by default bytes as utf8 strings decoder)
db = redis.database(decoder=utf8_bytes_as_strings)
# Shortcut so you don't have to type long words each time
c = db.command
cr = db.commandreply
# Send an pipelined SET request where you don't care about the result (You don't have to use bytes notation or caps)
c(b'SET', 'Hello', 'World!')
# Send a pipelined GET request and resolve it immediately
print('Hello, %s' % c('get', 'Hello')())
# or
print('Hello, %s' % cr('get', 'Hello'))
# You can even send both commands together atomically (so if the first fails the second won't run)
with db.multi() as m:
    m.command(b'SET', 'Hello', 'World!')
    hello = m.command('get', 'Hello')
print('Atomic Hello, %s' % hello())
```

*!!!WIP!!!* This example uses the Python 3 async/await syntax. Here you have an option to await on command for the sending part and await on calling () on it's result for the reply part (if you care about it). A shortcut for await on both is to use commandreply instead of command.

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
This is all the Python 2.7 compatible API in a nutshell:

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

You can also manually set the execution environment by the os environment variable JUSTREDIS_ENVIRONMENT:

* builtin - cooperative multitasking
* gevent - gevent greenlets (needs gevent installed)
* builtin_with_threads - preemptive multithreading

# Partly inspired by
The .NET Redis client package [ServiceStack.Redis](https://stackexchange.github.io/StackExchange.Redis/)
