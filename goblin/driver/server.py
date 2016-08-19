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
import aiohttp

from goblin.driver import connection


async def connect(url, loop, *, ssl_context=None, username='', password='',
                  lang='gremlin-groovy', aliases=None, session=None):
    connector = aiohttp.TCPConnector(ssl_context=ssl_context, loop=loop)
    client_session = aiohttp.ClientSession(loop=loop, connector=connector)
    ws = await client_session.ws_connect(url)
    return connection.Connection(url, ws, loop, client_session,
                                 aliases=aliases, lang=lang, session=session,
                                 username=username, password=password)


class GremlinServer:
    """
    Factory class that generates connections to the Gremlin Server. Do
    not instantiate directly, instead use :py:meth:GremlinServer.open
    """

    def __init__(self, conn, *, ssl_context=None,
                 username='', password='', lang='gremlin-groovy',
                 aliases=None, session=None):
        self._conn = conn
        self._url = self._conn.url
        self._loop = self._conn._loop
        self._ssl_context = ssl_context
        self._username = username
        self._password = password
        self._lang = lang
        self._aliases = aliases
        self._session = session

    async def close(self):
        await self._conn.close()

    async def connect(self):
        # This will use pool eventually
        if self._conn.closed:
            self._conn = self.get_connection(username=self._username,
                                             password=self._password,
                                             lang=self._lang,
                                             aliases=self._aliases,
                                             session=self._session)
        return self._conn

    @classmethod
    async def open(cls, url, loop, *, ssl_context=None,
                   username='', password='', lang='gremlin-groovy',
                   aliases=None, session=None, **kwargs):

        conn = await connect(url, loop, ssl_context=ssl_context,
                             username=username, password=password, lang=lang,
                             aliases=aliases, session=session)
        return cls(conn, ssl_context=ssl_context, username=username,
                   password=password, lang=lang, aliases=aliases,
                   session=session)


    async def get_connection(self, username=None, password=None, lang=None,
                             aliases=None, session=None):
        """
        Open a connection to the Gremlin Server.

        :param str url: Database url
        :param asyncio.BaseEventLoop loop: Event loop implementation
        :param str username: Username for server auth
        :param str password: Password for server auth

        :returns: :py:class:`Connection<goblin.driver.connection.Connection>`
        """
        username = username or self._username
        password = password or self._password
        aliasess = aliases or self._aliases
        session = session or self._session
        lang = lang or self._lang
        return await connect(self._url, self._loop,
                             ssl_context=self._ssl_context,
                             username=username, password=password, lang=lang,
                             aliases=aliases, session=session)
