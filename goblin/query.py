class Query:

    def __init__(self, session, element_class):
        self._session = session
        self._engine = session.engine
        if element_class.__type__ == 'vertex':
            self._traversal = self._session.g.V().hasLabel(
                element_class.__mapping__.label)
        elif element_class.__type__ == 'edge':
            self._traversal = self._session.g.E().hasLabel(
                element_class.__mapping__.label)
        else:
            raise Exception("unknown element type")

    # Generative query methods...
    def filter(self, **kwargs):
        raise NotImplementedError

    # Methods that issue a query
    async def all(self):
        script = self._traversal.translator.traversal_script
        stream = await self._engine.execute(
            script, bindings=self._traversal.bindings)
        # This should return and async iterator wrapper that can see and update
        # parent session object, but for the demo it works
        return stream
