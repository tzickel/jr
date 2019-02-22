global_env_default = "builtin"


global_env = "notset"
global_set = False


def builtin():
    import socket
    from threading import Lock
    from select import select

    env = {}
    env["socket"] = socket
    env["Lock"] = Lock
    env["select"] = select
    return env


def gevent():
    raise NotImplementedError("Not implmented yet")


def get_env(*args):
    global global_env, global_set
    if not global_set:
        global_env = global_env_default
        global_set = True
    env = globals()[global_env]()
    if len(args) == 1:
        return env.get(args[0])
    return [env.get(x) for x in args]


def set_env(env):
    global global_env, global_set
    if global_set:
        raise RuntimeError("Environment already set")
    global_set = True
    global_env = env
