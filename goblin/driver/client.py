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


class Client:
    """
    Client that utilizes a :py:class:`Cluster<goblin.driver.cluster.Cluster>`
    to access a cluster of Gremlin Server hosts. Issues requests to hosts using
    a round robin strategy.

    :param goblin.driver.cluster.Cluster cluster: Cluster used by
        client
    :param asyncio.BaseEventLoop loop:
    """
    def __init__(self, cluster, loop):
        self._cluster = cluster
        self._loop = loop
        self._traversal_source = {}

    def set_traversal_source(self, traversal_source):
        self._traversal_source = traversal_source

    @property
    def cluster(self):
        """
        Readonly property.

        :returns: The instance of
            :py:class:`Cluster<goblin.driver.cluster.Cluster>` associated with
            client.
        """
        return self._cluster

    def alias(self, traversal_source):
        client = Client(self._cluster, self._loop)
        client.set_traversal_source(traversal_source)
        return client

    async def submit(self,
                     *,
                     processor='',
                     op='eval',
                     **args):
        """
        **coroutine** Submit a script and bindings to the Gremlin Server.
        :param str processor: Gremlin Server processor argument
        :param str op: Gremlin Server op argument
        :param args: Keyword arguments for Gremlin Server. Depend on processor
            and op.

        :returns: :py:class:`Response` object
        """
        conn = await self.cluster.get_connection()
        resp = await conn.submit(
            processor=processor, op=op, **args)
        self._loop.create_task(conn.release_task(resp))
        return resp
