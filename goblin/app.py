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

"""Goblin application class and class constructor"""

import collections
import logging

from gremlin_python import process
from goblin import driver, element, session


logger = logging.getLogger(__name__)


async def create_app(url, loop, get_hashable_id=None, **config):
    """
    Constructor function for :py:class:`Goblin`. Connect to database and
    build a dictionary of relevant vendor implmentation features.

    :param str url: Database url
    :param asyncio.BaseEventLoop loop: Event loop implementation
    :param dict config: Config parameters for application

    :returns: :py:class:`Goblin` object
    """

    features = {}
    async with await driver.GremlinServer.open(url, loop) as conn:
        # Propbably just use a parser to parse the whole feature list
        aliases = config.get('aliases', {})
        stream = await conn.submit(
            'graph.features().graph().supportsComputer()', aliases=aliases)
        msg = await stream.fetch_data()
        features['computer'] = msg
        stream = await conn.submit(
            'graph.features().graph().supportsTransactions()', aliases=aliases)
        msg = await stream.fetch_data()
        features['transactions'] = msg
        stream = await conn.submit(
            'graph.features().graph().supportsPersistence()', aliases=aliases)
        msg = await stream.fetch_data()
        features['persistence'] = msg
        stream = await conn.submit(
            'graph.features().graph().supportsConcurrentAccess()', aliases=aliases)
        msg = await stream.fetch_data()
        features['concurrent_access'] = msg
        stream = await conn.submit(
            'graph.features().graph().supportsThreadedTransactions()', aliases=aliases)
        msg = await stream.fetch_data()
        features['threaded_transactions'] = msg
    return Goblin(url, loop, get_hashable_id=get_hashable_id,
                  features=features, **config)


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

    DEFAULT_CONFIG = {
        'translator': process.GroovyTranslator('g')
    }

    def __init__(self, url, loop, *, get_hashable_id=None, features=None,
                 **config):
        self._url = url
        self._loop = loop
        self._features = features
        self._config = self.DEFAULT_CONFIG
        self._config.update(config)
        self._vertices = collections.defaultdict(
            lambda: element.GenericVertex)
        self._edges = collections.defaultdict(lambda: element.GenericEdge)
        if not get_hashable_id:
            get_hashable_id = lambda x: x
        self._get_hashable_id = get_hashable_id

    @property
    def vertices(self):
        """Registered vertex classes"""
        return self._vertices

    @property
    def edges(self):
        """Registered edge classes"""
        return self._edges

    @property
    def features(self):
        """Vendor specific database implementation features"""
        return self._features

    def from_file(filepath):
        """Load config from filepath. Not implemented"""
        raise NotImplementedError

    def from_obj(obj):
        """Load config from object. Not implemented"""
        raise NotImplementedError

    @property
    def translator(self):
        """gremlin-python translator class"""
        return self._config['translator']

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

    async def session(self, *, use_session=False):
        """
        Create a session object.

        :param bool use_session: Create a database session. Not implemented

        :returns: :py:class:`Session<goblin.session.Session>` object
        """
        aliases = self._config.get('aliases', None)
        conn = await driver.GremlinServer.open(self.url, self._loop)
        return session.Session(self,
                               conn,
                               self._get_hashable_id,
                               use_session=use_session,
                               aliases=aliases)
