# Why?
* All commands are pipelined (asynchronous)
* Single connection per redis instance even between different execution contexts (extra one if using pub/sub)
* Per command encoding / decoding / retries

# Inherit Limitations
* Cannot issue blocking commands (such as BLPOP)
* Cannot issue transaction commands with WATCH (but MULTI/EXEC can be used)

# Current Limitations
* No cluster support
* No pub/sub support

# Example
```python
from justredis import Multiplexer, utf8_bytes_as_strings
# Connect to the default redis port on localhost
redis = Multiplexer()
# Send commands to database #0 (and use by default bytes as strings decoder)
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
    g = m.command('get', 'Hello')
print('Atomic Hello, %s' % g())
```
