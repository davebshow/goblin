# Copyright 2016 ZEROFAIL
#
# This file is part of Goblin.
#
# Goblin is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Goblin is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with Goblin.  If not, see <http://www.gnu.org/licenses/>.

import asyncio
import collections
import configparser
import ssl

try:
    import ujson as json
except ImportError:
    import json

import yaml

from goblin import driver, exception


def my_import(name):
    components = name.split('.')
    mod = __import__(components[0])
    for comp in components[1:]:
        mod = getattr(mod, comp)
    return mod


class Cluster:
    """
    A cluster of Gremlin Server hosts. This object provides the main high
    level interface used by the :py:mod:`goblin.driver` module.

    :param asyncio.BaseEventLoop loop:
    """

    DEFAULT_CONFIG = {
        'scheme': 'ws',
        'hosts': ['localhost'],
        'port': 8182,
        'ssl_certfile': '',
        'ssl_keyfile': '',
        'ssl_password': '',
        'username': '',
        'password': '',
        'response_timeout': None,
        'max_conns': 4,
        'min_conns': 1,
        'max_times_acquired': 16,
        'max_inflight': 64,
        'message_serializer': 'goblin.driver.GraphSON2MessageSerializer'
    }

    def __init__(self, loop, aliases=None, **config):
        self._loop = loop
        self._config = self._get_message_serializer(dict(self.DEFAULT_CONFIG))
        self._config.update(config)
        self._hosts = collections.deque()
        self._closed = False
        if aliases is None:
            aliases = {}
        self._aliases = aliases

    @classmethod
    async def open(cls, loop, *, aliases=None, configfile=None, **config):
        """
        **coroutine** Open a cluster, connecting to all available hosts as
        specified in configuration.

        :param asyncio.BaseEventLoop loop:
        :param str configfile: Optional configuration file in .json or
            .yml format
        """
        cluster = cls(loop, aliases=aliases, **config)
        if configfile:
            cluster.config_from_file(configfile)
        await cluster.establish_hosts()
        return cluster

    @property
    def hosts(self):
        return self._hosts

    @property
    def config(self):
        """
        Readonly property.

        :returns: `dict` containing the cluster configuration
        """
        return self._config

    async def get_connection(self):
        """
        **coroutine** Get connection from next available host in a round robin
        fashion.

        :returns: :py:class:`Connection<goblin.driver.connection.Connection>`
        """
        if not self._hosts:
            await self.establish_hosts()
        host = self._hosts.popleft()
        conn = await host.get_connection()
        self._hosts.append(host)
        return conn

    async def establish_hosts(self):
        """
        **coroutine** Connect to all hosts as specified in configuration.
        """
        scheme = self._config['scheme']
        hosts = self._config['hosts']
        port = self._config['port']
        for host in hosts:
            url = '{}://{}:{}/gremlin'.format(scheme, host, port)
            host = await driver.GremlinServer.open(
                url, self._loop, **dict(self._config))
            self._hosts.append(host)

    def config_from_file(self, filename):
        """
        Load configuration from from file.

        :param str filename: Path to the configuration file.
        """
        if filename.endswith('yml') or filename.endswith('yaml'):
            self.config_from_yaml(filename)
        elif filename.endswith('.json'):
            self.config_from_json(filename)
        else:
            raise exception.ConfigurationError('Unknown config file format')

    def config_from_yaml(self, filename):
        with open(filename, 'r') as f:
            config = yaml.load(f)
        config = self._get_message_serializer(config)
        self._config.update(config)

    def config_from_json(self, filename):
        """
        Load configuration from from JSON file.

        :param str filename: Path to the configuration file.
        """
        with open(filename, 'r') as f:
            config = json.load(f)
        config = self._get_message_serializer(config)
        self.config.update(config)

    def _get_message_serializer(self, config):
        message_serializer = config.get('message_serializer', '')
        if message_serializer:
            config['message_serializer'] = my_import(message_serializer)
        return config

    def config_from_module(self, filename):
        raise NotImplementedError

    async def connect(self, processor=None, op=None, aliases=None,
                      session=None):
        """
        **coroutine** Get a connected client. Main API method.

        :returns: A connected instance of `Client<goblin.driver.client.Client>`
        """
        aliases = aliases or self._aliases
        if not self._hosts:
            await self.establish_hosts()
        if session:
            host = self._hosts.popleft()
            client = driver.SessionedClient(host, self._loop, session,
                                            aliases=aliases)
            self._hosts.append(host)
        else:
            client = driver.Client(self, self._loop, processor=processor,
                                   op=op, aliases=aliases)
        return client

    async def close(self):
        """**coroutine** Close cluster and all connected hosts."""
        waiters = []
        while self._hosts:
            host = self._hosts.popleft()
            waiters.append(host.close())
        await asyncio.gather(*waiters)
        self._closed = True
