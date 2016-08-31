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
                     mime_type='application/json',
                     **args):
        """
        **coroutine** Submit a script and bindings to the Gremlin Server.
        :param str processor: Gremlin Server processor argument
        :param str op: Gremlin Server op argument
        :param args: Arguments for Gremlin Server. Depend on processor and
            op.
        :returns: :py:class:`Response` object
        """
        conn = await self.cluster.get_connection()
        resp = await conn.submit(
            processor=processor, op=op, mime_type=mime_type, **args)
        self._loop.create_task(conn.release_task(resp))
        return resp

    async def close(self):
        """**coroutine** Close client and ???cluster???"""
        self._cluster = None
