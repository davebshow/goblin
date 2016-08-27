class Client:

    def __init__(self, cluster, loop):
        self._cluster = cluster
        self._loop = loop

    @property
    def cluster(self):
        return self._cluster

    async def submit(self,
                     gremlin,
                     *,
                     bindings=None,
                     lang=None,
                     traversal_source=None,
                     session=None):
        conn = await self.cluster.get_connection()
        resp = await conn.submit(gremlin,
                                 bindings=bindings,
                                 lang=lang,
                                 traversal_source=traversal_source,
                                 session=session)
        self._loop.create_task(conn.release_task(resp))
        return resp

    async def close(self):
        self._cluster = None
