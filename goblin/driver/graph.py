from goblin.gremlin_python.process.graph_traversal import GraphTraversalSource, GraphTraversal
from goblin.gremlin_python.process.traversal import TraversalStrategy, TraversalStrategies


class AsyncGraphTraversal(GraphTraversal):
    def __init__(self, graph, traversal_strategies, bytecode):
        GraphTraversal.__init__(self, graph, traversal_strategies, bytecode)

    def __repr__(self):
        return self.graph.translator.translate(self.bytecode)

    def toList(self):
        raise NotImplementedError

    def toSet(self):
        raise NotImplementedError

    async def next(self):
        resp = await self.traversal_strategies.apply(self)
        return resp


class AsyncRemoteStrategy(TraversalStrategy):
    async def apply(self, traversal):
        result = await traversal.graph.remote_connection.submit(
            traversal.graph.translator.translate(traversal.bytecode),
            bindings=traversal.bindings,
            lang=traversal.graph.translator.target_language)
        return result


class AsyncGraph(object):
    def traversal(self):
        return GraphTraversalSource(self, self.traversal_strategy,
                                    graph_traversal=AsyncGraphTraversal)


class AsyncRemoteGraph(AsyncGraph):
    def __init__(self, translator, remote_connection):
        self.traversal_strategy = AsyncRemoteStrategy()  # A single traversal strategy
        self.translator = translator
        self.remote_connection = remote_connection

    def __repr__(self):
        return "remotegraph[" + self.remote_connection.url + "]"
