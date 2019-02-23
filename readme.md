# What?
An asynchronous redis client library for Python 2.7 and 3.5+

# Why?
* All commands are pipelined (asynchronous)
* No connection pool, a single connection per redis instance even between different execution contexts (extra one if using pub/sub)
* Optional per command encoding / decoding / retries

# Also supports
* Pub/Sub
* Transparent script caching
* Retry support only when it's safe
* Hiredis parser
* Testing with private redis server

# Inherit Limitations
* Cannot issue blocking commands (such as BLPOP)
* Cannot issue transaction commands with WATCH (but MULTI and EXEC can be used)

# Roadmap
- [ ] Cluster support
- [ ] async/await support
- [ ] Support for non cooperative result parsing
- [ ] API Finalization
- [ ] Resp V3 ?

# Example
This example uses an cooperative asynchronous syntax which is compatible with Python 2.7. You can call () a redis command to resolve it's reply (or not if you don't care about the result).

```python
from justredis import Multiplexer, utf8_bytes_as_strings
# Connect to the default redis port on localhost
redis = Multiplexer()
# Send commands to database #0 (and use by default bytes as utf8 strings decoder)
db = redis.database(decoder=utf8_bytes_as_strings)
# Shortcut so you don't have to type long words each time
c = db.command
# Send an pipelined SET request where you don't care about the result (You don't have to use bytes notation or caps)
c(b'SET', 'Hello', 'World!')
# Send a pipelined GET request and resolve it immediately
print('Hello, %s' % c('get', 'Hello')())
# You can even send both commands together atomically (so if the first fails the second won't run)
with db.multi() as m:
    m.command(b'SET', 'Hello', 'World!')
    hello = m.command('get', 'Hello')
print('Atomic Hello, %s' % hello())
```

*WIP* This example uses the Python 3 async/await syntax. Here you have an option to await on command for the sending part and await on calling () on it's result for the reply part (if you care about it). A shortcut for await on both is to use commandreply instead of command.

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
