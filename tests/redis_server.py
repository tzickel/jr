import subprocess
import random


def start_cluster(masters, dockerimage=None, extraparams='', ipv4=True):
    addr = '127.0.0.1' if ipv4 else '::1'
    ret = []
    subprocess.call('rm /tmp/justredis_cluster*.conf', shell=True)
    for x in range(masters):
        ret.append(RedisServer(extraparams='--cluster-enabled yes --cluster-config-file /tmp/justredis_cluster%d.conf' % x))
    subprocess.Popen('redis-cli --cluster create ' + ' '.join(['%s:%d' % (addr, server.port) for server in ret]), stdin=subprocess.PIPE, shell=True).communicate(b'yes\n')
    return ret


class RedisServer(object):
    def __init__(self, dockerimage=None, extraparams=''):
        while True:
            self._proc = None
            self._port = random.randint(1025, 65535)
            if dockerimage:
                cmd = 'docker run --rm -p {port}:6379 {image} --save {extraparams}'.format(port=self.port, image=dockerimage, extraparams=extraparams)
            else:
                cmd = 'redis-server --save --port {port} {extraparams}'.format(port=self.port, extraparams=extraparams)
            self._proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            seen_redis = False
            while True:
                line = self._proc.stdout.readline()
                if b'Redis' in line:
                    seen_redis = True
                if line == b'':
                    self._port = None
                    break
                elif b'Ready to accept connections' in line:
                    break
                elif b'Opening Unix socket' in line and b'Address already in use' in line:
                    raise Exception('Unix domain already in use')
            # usually could not find docker image
            if not seen_redis:
                raise Exception('Could not run redis')
            if self._port:
                break

    @property
    def port(self):
        return self._port

    def __del__(self):
        if self._proc:
            try:
                self._proc.stdout.close()
                self._proc.kill()
                self._proc.wait()
            except Exception:
                pass
            self._proc = None
