# An error response from the redis server
class RedisReplyError(Exception):
    pass

# An error from this library
class RedisError(Exception):
    pass