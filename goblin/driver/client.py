class Client:

    def __init__(self, cluster):
        self._cluster = cluster

    @property
    def cluster(self):
        return self._cluster

    async def submit(self,
                     gremlin,
                     *,
                     bindings=None,
                     lang=None,
                     aliases=None,
                     session=None):
        conn = await self.cluster.get_connection()
        resp = await conn.submit(gremlin,
                                 bindings=bindings,
                                 lang=lang,
                                 aliases=aliases,
                                 session=session)
        return resp

    async def close(self):
        await self._cluster.close()
        self._cluster = None
