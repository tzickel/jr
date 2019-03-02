import sys
import os


global_env_default = "builtin"
if 'gevent' in sys.modules:
    global_env_default = "gevent"
elif 'threading' in sys.modules:
    global_env_default = "builtin_with_threads"


os_environ = os.environ.get('JUSTREDIS_ENVIRONMENT')
if os_environ is not None:
    global_env_default = os_environ


global_env = "notset"
global_set = False


def builtin():
    import socket
    from threading import Lock, Event
    from select import select

    env = {}
    env["socket"] = socket
    env["Lock"] = Lock
    env["Event"] = Event
    env["select"] = select
    env["thread"] = None
    return env


def builtin_with_threads():
    import socket
    from threading import Lock, Event, Thread
    from select import select

    def thread(target):
        t = Thread(target=target)
        t.daemon = True
        t.start()
        return t

    env = {}
    env["socket"] = socket
    env["Lock"] = Lock
    env["Event"] = Event
    env["select"] = select
    env["thread"] = thread
    return env


def gevent():
    from gevent import socket
    from gevent.lock import Semaphore
    from gevent.event import Event
    from gevent.select import select
    from gevent import spawn

    env = {}
    env["socket"] = socket
    env["Lock"] = Semaphore
    env["Event"] = Event
    env["select"] = select
    env["thread"] = lambda x: spawn(x)
    return env


def get_env(*args):
    global global_env, global_set
    if not global_set:
        global_env = global_env_default
        global_set = True
    env = globals().get(global_env)
    if env:
        env = env()
    else:
        raise ValueError('No such environment definition: %s' % global_env)
    if len(args) == 1:
        return env.get(args[0])
    return [env.get(x) for x in args]


def set_env(env):
    global global_env, global_set
    if global_set:
        raise RuntimeError("Environment already set")
    global_set = True
    global_env = env
