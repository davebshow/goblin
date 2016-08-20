import asyncio
import collections
import ssl

from goblin import driver


class Cluster:

    DEFAULT_CONFIG = {
        'scheme': 'ws',
        'hosts': ['localhost'],
        'port': 8182,
        'ssl_certfile': '',
        'ssl_keyfile': '',
        'ssl_password': '',
        'username': '',
        'password': ''
    }

    def __init__(self, loop, **config):
        self._loop = loop
        self._config = self.DEFAULT_CONFIG
        self._config.update(config)
        self._hosts = collections.deque()
        self._closed = False

    @classmethod
    async def open(cls, loop, *, inifile=None, jsonfile=None,
                   modulename=None, **config):
        cluster = cls(loop, **config)
        if inifile:
            cluster.config_from_ini(inifile)
        if jsonfile:
            cluster.config_from_json(jsonfile)
        if modulename:
            cluster.config_from_module(modulename)
        await cluster.establish_hosts()
        return cluster

    @property
    def config(self):
        return self._config

    async def get_connection(self):
        """Get connection from next available host in a round robin fashion"""
        host = self._hosts.popleft()
        conn = await host.connect()
        self._hosts.append(host)
        return conn

    async def establish_hosts(self):
        scheme = self._config['scheme']
        hosts = self._config['hosts']
        port = self._config['port']
        username = self._config['username']
        password = self._config['password']
        if scheme in ['https', 'wss']:
            certfile = self._config['ssl_certfile']
            keyfile = self._config['ssl_keyfile']
            ssl_password = self._config['ssl_password']
            ssl_context = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
            ssl_context.load_cert_chain(
                certfile, keyfile=keyfile, password=ssl_password)
        else:
            ssl_context = None
        for host in hosts:
            url = '{}://{}:{}/'.format(scheme, host, port)
            host = await driver.GremlinServer.open(
                url, self._loop, ssl_context=ssl_context, username=username,
                password=password)
            self._hosts.append(host)

    def config_from_ini(self, inifile):
        pass

    def config_from_json(self, jsonfile):
        pass

    def config_from_module(self, modulename):
        pass

    async def connect(self):
        if not self._hosts:
            await self.establish_hosts()
        return driver.Client(self)

    async def close(self):
        waiters = []
        while self._hosts:
            host = self._hosts.popleft()
            waiters.append(host.close())
        await asyncio.gather(*waiters)
        self._closed = True
