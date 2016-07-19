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


class GremlinServer:
    """Factory class that generates connections to the Gremlin Server"""

    @classmethod
    async def open(cls,
                   url,
                   loop,
                   *,
                   client_session=None,
                   username=None,
                   password=None):
        """
        Open a connection to the Gremlin Server.

        :param str url: Database url
        :param asyncio.BaseEventLoop loop: Event loop implementation
        :param aiohttp.client.ClientSession client_session: Client session
            used to generate websocket connections.
        :param str username: Username for server auth
        :param str password: Password for server auth

        :returns: :py:class:`Connection<goblin.driver.connection.Connection>`
        """
        if client_session is None:
            client_session = aiohttp.ClientSession(loop=loop)
        ws = await client_session.ws_connect(url)
        return connection.Connection(url, ws, loop, client_session,
                                     username=username, password=password)
