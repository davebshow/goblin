# Copyright 2016 David M. Brown
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

"""Goblin application class and class constructor"""

import collections
import importlib
import logging

import aiogremlin
from goblin import element, provider, session


logger = logging.getLogger(__name__)


# Main API classes
class Goblin:
    """
    Class used to encapsulate database connection configuration and generate
    database connections Used as a factory to create
    :py:class:`Session<goblin.session.Session>` objects.

    :param str url: Database url
    :param asyncio.BaseEventLoop loop: Event loop implementation
    :param dict features: Vendor implementation specific database features
    :param dict config: Config parameters for application
    """

    def __init__(self, cluster, *, provider=provider.TinkerGraph, get_hashable_id=None, aliases=None):
        self._cluster = cluster
        self._loop = self._cluster._loop
        self._cluster = cluster
        self._vertices = collections.defaultdict(
            lambda: element.GenericVertex)
        self._edges = collections.defaultdict(lambda: element.GenericEdge)
        self._provider = provider
        if not get_hashable_id:
            get_hashable_id = self._provider.get_hashable_id
        self._get_hashable_id = get_hashable_id
        if aliases is None:
            aliases = {}
        self._aliases = aliases

    @classmethod
    async def open(cls, loop, *, provider=provider.TinkerGraph,
                   get_hashable_id=None, aliases=None, **config):
        # App currently only supports GraphSON 1
        # aiogremlin does not yet support providers
        cluster = await aiogremlin.Cluster.open(
            loop, aliases=aliases,
            **config)
        app = Goblin(cluster, provider=provider,
                     get_hashable_id=get_hashable_id, aliases=aliases)
        return app

    @property
    def cluster(self):
        return self._cluster

    @property
    def config(self):
        return self.cluster.config

    @property
    def vertices(self):
        """Registered vertex classes"""
        return self._vertices

    @property
    def edges(self):
        """Registered edge classes"""
        return self._edges

    @property
    def url(self):
        """Database url"""
        return self._url

    def register(self, *elements):
        """
        Register user created Element classes.

        :param goblin.element.Element elements: User defined Element classes
        """
        for element in elements:
            if element.__type__ == 'vertex':
                self._vertices[element.__label__] = element
            if element.__type__ == 'edge':
                self._edges[element.__label__] = element

    def config_from_file(self, filename):
        """
        Load configuration from from file.

        :param str filename: Path to the configuration file.
        """
        self._cluster.config_from_file(filename)

    def config_from_yaml(self, filename):
        self._cluster.config_from_yaml(filename)

    def config_from_json(self, filename):
        """
        Load configuration from from JSON file.

        :param str filename: Path to the configuration file.
        """
        self._cluster.config_from_json(filename)

    def config_from_module(self, module):
        self._cluster.config_from_module(module)

    def register_from_module(self, module, *, package=None):
        if isinstance(module, str):
            module = importlib.import_module(module, package)
        elements = list()
        for item_name in dir(module):
            item = getattr(module, item_name)
            if isinstance(item, element.ElementMeta):
                elements.append(item)
        self.register(*elements)

    async def session(self, *, processor='', op='eval',
                      aliases=None):
        """
        Create a session object.

        :returns: :py:class:`Session<goblin.session.Session>` object
        """
        remote_connection = await aiogremlin.DriverRemoteConnection.using(
                self._cluster, aliases=self._aliases)
        return session.Session(self,
                               remote_connection,
                               self._get_hashable_id)

    async def close(self):
        await self._cluster.close()
