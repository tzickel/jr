## What?
An asynchronous redis client library for Python 3.6+

Please note that this project is currently alpha quality and the API is not finalized. Please provide feedback if you think the API is convenient enough or not. A permissive license will be chosen once the API will be more mature for wide spread consumption.

## Why?
### All commands are pipelined by default

Since most Redis server commands are intended to be run for a short time on the server, sometimes you pay more time on network latency than execution time. By splitting the sending and receiving parts into seperate coroutines all commands are sent seperately from waiting for their response.

### Transparent API

The library focuses on hiding from you the internals of Redis behaviour such as pipelining, cluster support, script caching, slow and blocking and multi commands, connection pooling, publish and subscribe.

You can see the example below for some examples.

### Missing

The library currently does not focus on providing the following:

1. A higher level API for understanding the different commands responses
2. Sentinal support
3. SSL connections
4. Implementing high-level constructs such as distributed locks on top of redis (Not in the scope of this project)

## Roadmap
- [ ] API Finalization
- [ ] Choose license
- [ ] Resolve all TODO in code
- [ ] More test coverage and test out network I/O failure and concurrency

## Installing
For now you can install this via this github repository by pip installing or adding to your requirements.txt file:

```
git+git://github.com/tzickel/justredis@master#egg=justredis
```

Replace master with the specific branch or version tag you want.

## Examples
```python
from justredis import MultiplexerPool, utf8_bytes_as_strings
import asyncio


async def main():
    # Connect to the default redis port on localhost
    async with MultiplexerPool() as redis:
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

        # This shows support in the Pooled multiplexer for blocking commands
        waiting = await c(b'BLPOP', 'queue', 0)
        await cr(b'RPUSH', 'queue', 'Hello, World!')
        print('Queued %s' % (await waiting())[1])

        # And even with publish & subscribe.
        async with redis.pubsub(decoder=utf8_bytes_as_strings) as pubsub:
            await pubsub.add('Hello')
            await cr(b'PUBLISH', 'Hello', 'World!')
            await pubsub.message() # This is the registration message for the Hello channel.
            print('PubSub Hello, %s' % (await pubsub.message())[2])

        # And here we can do an atomic get and increment example
        async with db.watch('Counter') as w:
            value = int(await w.commandreply(b'GET', 'Counter') or 0)
            value += 1
            async with w.multi() as m:
                m.command(b'SET', 'Counter', value)
            # If there is an transaction error, it will throw here
            counter = value


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.close()
```

You can check the [tests](tests/test.py) for some more examples.

## API
This is all the API in a nutshell:

```python
MultiplexerPool(configuration=None) or Multiplexer(configuration=None)
  async __aenter__()
  async __aexit__()
  async aclose()
  database(number=0, encoder=utf8_encode, decoder=None)
    async command(*args, encoder=utf8_encode, decoder=None, throw=True)
      async __call__() # You can also await directly on the command as well
    async commandreply(*args, encoder=utf8_encode, decoder=None, throw=True)
    multi()
      async __aenter__()
      async __aexit__()
      # Notice that this command is not awaitable
      command(*args, encoder=utf8_encode, decoder=None, throw=True)
        # But the result is
        async __call__() # You can also await directly on the command as well
      # This command will be automatically called when leaving the context manager
      async execute()
      # This command will be automatically called when leaving the context manager on exception (or can be called explicitly before to abort)
      async discard()
    watch(*keys)
      async __aenter__()
      async __aexit__()
      async aclose()
      async command() # Like above
      async commandreply() # like above
      multi() # Like above
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

MultiplexerPool also has this parameters:

* maxconnections - maximum number of in use connections before blocking new requests.

## Redis command replacements
Some redis commands are not allowed to be run directly, this chapter explains which commands, why, and what is their replacment.

### Database selection (SELECT)

Since the client can multiplex multiple commands into one socket, it's important to keep track on which database number each command is running.

Each multiplexer has a `database(number=0, encoder=utf8_encode, decoder=None)` command which keeps track on which database number the commands are running.

All the redis commands should be called via the Database object returned by database.

```python
db = redis.database() # you can set database(2) for database #2
a = await db.cr('get', 'a')
```

### Transction (MULTI / EXEC / DISCARD)

Since the client can multiplex multiple commands into one socket, and a transaction is a group of commands which is executed together atomically, it's important not to mix them up with other commands.

Each database has an `multi()` command which you can run commands you want as part of a transaction together.

It's important to see that the commands API is not awaitable inside a transaction scope, since they actually run in the end (execute part). You await for their result outside the transaction scope.

```python
db = redis.database()
async with db.multi() as m:
    m.commmand('set', 'a', 'b')
    a = m.command('get', 'a')
a = await a
```

### Conditional transaction (WATCH)

Conditional transactions require to send each command between the WATCH and MULTI part directly and get the result, thus that part has additional interface above the regular transaction one above.

The database has an `watch(*keys)` command which starts a conditional transaction.

```python
db = redis.database()
async with db.watch('Counter') as w:
    value = int(await w.commandreply(b'GET', 'Counter') or 0)
    value += 1
    async with w.multi() as m:
        m.command(b'SET', 'Counter', value)
    # If there is an transaction error, it will throw here
    counter = value
```

### Password (AUTH)

Since the multiplexer handles reconnection, you should not manualy call the AUTH command to authenticate with the server.

If your redis server has password authentication, then pass to the multiplexer constructor the password argument.

### Subscribe (SUBSCRIBE/ PSUBSCRIBE / UNSUBSCRIBE / PUNSUBSCRIBE)

The multiplexer combines all the subscribed channels and patterns into one socket. It provides a high level API to manage it.

If you want to subscribe to topics (channels and patterns), use the multiplexer `pubsub()` command, which returns an instance you can `add(channels, patterns)` or `remove(channels, patterns)`

You can then call `message(timeout)` to wait for a message on one of the registered topics.

If there is an I/O error while waiting for a message, an Exception will be thrown. Attempting to call `message` again, will attempt to reconnect and reestablish the listened topics.

```python
async with redis.pubsub(decoder=utf8_bytes_as_strings) as pubsub:
    await pubsub.add('Some Channel')
    while True:
        msg = await pubsub.message()
```

### Scripting (EVAL)

EVAL commands, are first tried to be run as EVALSHA commands (i.e. only their hash is sent to the server), if the attempt fails (the server does not know this script), the client auto sends the request as an EVAL again.

### Debugging (MONITOR)

Not implmented yet (can be done like Subscribe)

## Additional commands

### Cluster API

The Multiplexer has the command `run_commandreply_on_all_masters()` which runs the given command on all the cluster masters and returns the result as a dictionary with each server has an entry.

## Partially inspired by
The .NET Redis client package [StackExchange.Redis](https://stackexchange.github.io/StackExchange.Redis/)
