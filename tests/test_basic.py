import pytest
from justredis import SyncRedis, Error


# TODO (misc) copy all of misc/example.py into here


def test_auth(client_with_blah_password):
    address = ('localhost', client_with_blah_password._settings['address'][1])
    # No password
    with SyncRedis(address=address) as r:
        with pytest.raises(Error) as exc_info:
            r('set', 'a', 'b')
        assert exc_info.value.args[0].startswith(b'NOAUTH')

    # Wrong password
    with SyncRedis(address=address, password='nop') as r:
        with pytest.raises(Error) as exc_info:
            r('set', 'a', 'b')
        # Changes between Redis 5 and Redis 6
        assert exc_info.value.args[0].startswith(b'WRONGPASS') or exc_info.value.args[0].startswith(b'ERR invalid password')

    # Correct password
    with SyncRedis(address=address, password='blah') as r:
        assert r('set', 'a', 'b') == b'OK'


def test_simple(client):
    assert client('set', 'a', 'a') == b'OK'
    assert client('set', 'b', 'b') == b'OK'
    assert client('set', 'c', 'c') == b'OK'
    assert client('set', '{a}b', 'd') == b'OK'
    assert client('get', 'a') == b'a'
    try:
        with client.database(1).connection('a') as c:
            c('set', 'a', 'a') == b'OK'
    except Error as e:
        if e.args[0] == b'ERR SELECT is not allowed in cluster mode':
            pass
        else:
            raise


def test_notallowed(client):
    with pytest.raises(Error) as exc_info:
        client('auth', 'asd')
    assert exc_info.value.args[0].startswith(b'ERR')


def test_some_encodings(client):
    with pytest.raises(ValueError):
        client('set', 'a', True)
    assert client('incrbyfloat', 'float_check', 0.1) == b'0.1'
    with pytest.raises(ValueError):
        client('set', 'a', [1, 2])
    client('set', '{check}_a', 'a')
    client('set', '{check}_b', 'b')
    assert client({'command': ('get', '{check}_a'), 'decoder': 'utf8'}) == 'a'
    assert client({'command': ('mget', '{check}_a', '{check}_b'), 'decoder': 'utf8'}) == ['a', 'b']


def test_chunk_encoded_command(client):
    assert client('set', 'test_chunk_encoded_command_a', b'test_chunk_encoded_command_a'*10*1024) == b'OK'
    assert client('get', 'test_chunk_encoded_command_a') == b'test_chunk_encoded_command_a'*10*1024
    assert client('mget', 'test_chunk_encoded_command_a' * 3500, 'test_chunk_encoded_command_a' * 3500, 'test_chunk_encoded_command_a' * 3500) == [None, None, None]


def test_eval(client):
    assert client('set', 'evaltest', 'a') == b'OK'
    assert client('eval', "return redis.call('get',KEYS[1])", 1, 'evaltest') == b'a'
    assert client('eval', "return redis.call('get',KEYS[1])", 1, 'evaltestno') == None
    assert client('eval', "return redis.call('get',KEYS[1])", 1, 'evaltest') == b'a'
    assert client('eval', "return redis.call('get',KEYS[1])", 1, 'evaltestno') == None
    assert client('script', 'flush') == b'OK'
    assert client('eval', "return redis.call('get',KEYS[1])", 1, 'evaltest') == b'a'
    assert client('eval', "return redis.call('get',KEYS[1])", 1, 'evaltestno') == None


# TODO (misc) add some extra checks here for invalid states
def test_multi(client):
    with client.connection('a') as c:
        c('multi')
        c('set', 'a', 'b')
        c('get', 'a')
        assert c('exec') == [b'OK', b'b']

    # TODO (misc) kinda lame
    try:
        with client.database(2).connection('a') as c1:
            c1('multi')
            c1('set', 'a', 'b')
            with client.database(3).connection('a') as c2:
                c2('multi')
                c2('set', 'a1', 'c')
                c2('get', 'a')
                assert c2('exec') == [b'OK', None]
            c1('mget', 'a', 'a1')
            assert c1('exec') == [b'OK', [b'b', None]]
        assert client.database(2)('get', 'a') == b'b'
    except Error as e:
        if e.args[0] == b'ERR SELECT is not allowed in cluster mode':
            pass
        else:
            raise


def test_multidiscard(client):
    with client.connection('a') as c:
        c('multi')
        with pytest.raises(Error):
            c('nothing')
        c('discard')
        c('multi')
        c('set', 'a', 'b')
        assert c('exec') == [b'OK']


def test_pubsub(client):
    with client.pubsub() as pubsub:
        with pytest.raises(Exception):
            pubsub.next_message()
        pubsub.subscribe('hi')
        pubsub.psubscribe(b'bye')
        assert pubsub.next_message() == [b'subscribe', b'hi', 1]
        assert pubsub.next_message() == [b'psubscribe', b'bye', 2]
        assert pubsub.next_message(0.1) == None
        client('publish', 'hi', 'there')
        assert pubsub.next_message(0.1) == [b'message', b'hi', b'there']
        client('publish', 'bye', 'there')
        assert pubsub.next_message(0.1) == [b'pmessage', b'bye', b'bye', b'there']
        pubsub.ping()
        # RESP2 and RESP3 behave differently here, so check for both
        assert pubsub.next_message() in (b'PONG', [b'pong', b''])
        pubsub.ping(b'hi')
        assert pubsub.next_message() in (b'hi', [b'pong', b'hi'])
        pubsub.unsubscribe('hi')


def test_misc(client):
    # This tests an command which redis server says keys start in index 2.
    client('object', 'help')
    # Check command with no keys
    client('client', 'list')


def test_server(client):
    # TODO split keys to 3 comps
    client('set', 'cluster_aa', 'a') == b'OK'
    client('set', 'cluster_bb', 'b') == b'OK'
    client('set', 'cluster_cc', 'c') == b'OK'
    # TODO this is bad, return ips
    result = client.on_all_masters('keys', 'cluster_*')
    # TODO check both cluster and not cluster
    if len(result) == 1:
        result = list(result.values())
        result = [i for s in result for i in s]
    else:
        assert len(result) == 3
        result = list(result.values())
        result = [i for s in result for i in s]
    assert set(result) == set([b'cluster_aa', b'cluster_bb', b'cluster_cc'])


def test_moved(client):
    client('set', 'aa', 'a') == b'OK'
    client('set', 'bb', 'b') == b'OK'
    client('set', 'cc', 'c') == b'OK'
    result = client.on_all_masters('get', 'aa')
    if len(result) == 1:
        result = list(result.values())
        assert result == [b'a']
    else:
        result = list(result.values())
        assert result == [b'a', b'a', b'a']