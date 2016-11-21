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

from goblin import exception


class Client:
    """
    Client that utilizes a :py:class:`Cluster<goblin.driver.cluster.Cluster>`
    to access a cluster of Gremlin Server hosts. Issues requests to hosts using
    a round robin strategy.

    :param goblin.driver.cluster.Cluster cluster: Cluster used by
        client
    :param asyncio.BaseEventLoop loop:
    """
    def __init__(self, cluster, loop, *, aliases=None, processor=None,
                 op=None):
        self._cluster = cluster
        self._loop = loop
        if aliases is None:
            aliases ={}
        self._aliases = aliases
        if processor is None:
            processor = ''
        self._processor = processor
        if op is None:
            op = 'eval'
        self._op = op

    @property
    def message_serializer(self):
        return self.cluster.config['message_serializer']

    @property
    def cluster(self):
        """
        Readonly property.

        :returns: The instance of
            :py:class:`Cluster<goblin.driver.cluster.Cluster>` associated with
            client.
        """
        return self._cluster

    def alias(self, aliases):
        client = Client(self._cluster, self._loop,
                        aliases=aliases)
        return client

    async def submit(self,
                     *,
                     processor=None,
                     op=None,
                     **args):
        """
        **coroutine** Submit a script and bindings to the Gremlin Server.

        :param str processor: Gremlin Server processor argument
        :param str op: Gremlin Server op argument
        :param args: Keyword arguments for Gremlin Server. Depend on processor
            and op.
        :returns: :py:class:`Response` object
        """
        processor = processor or self._processor
        op = op or self._op
        # Certain traversal processor ops don't support this arg
        if not args.get('aliases') and op not in ['keys', 'close',
                                                  'authentication']:
            args['aliases'] = self._aliases
        conn = await self.cluster.get_connection()
        resp = await conn.submit(
            processor=processor, op=op, **args)
        self._loop.create_task(conn.release_task(resp))
        return resp


class SessionedClient(Client):

    def __init__(self, cluster, loop, session, *, aliases=None):
        super().__init__(cluster, loop, aliases=aliases, processor='session',
                         op='eval')
        self._session = session

    @property
    def session(self):
        return self._session

    async def submit(self, **args):
        if not args.get('gremlin', ''):
            raise exception.ClientError('Session requires a gremlin string')
        return await super().submit(processor='session', op='eval',
                                    session=self.session,
                                    **args)

    async def close(self):
        raise NotImplementedError
