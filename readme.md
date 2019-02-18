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
