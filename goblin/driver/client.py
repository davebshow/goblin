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

    @property
    def cluster(self):
        """
        Readonly property.

        :returns: The instance of
            :py:class:`Cluster<goblin.driver.cluster.Cluster>` associated with
            client.
        """
        return self._cluster

    async def submit(self,
                     *,
                     processor='',
                     op='eval',
                     mime_type='application/json',
                     **args):
        """
        **coroutine** Submit a script and bindings to the Gremlin Server.

        :param str gremlin: Gremlin script to submit to server.
        :param dict bindings: A mapping of bindings for Gremlin script.
        :param str lang: Language of scripts submitted to the server.
            "gremlin-groovy" by default
        :param dict traversal_source: ``TraversalSource`` objects to different
            variable names in the current request.
        :param str session: Session id (optional). Typically a uuid

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
