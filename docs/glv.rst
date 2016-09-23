Using AsyncGraph (GLV)
======================

:py:mod:`Goblin` provides an asynchronous version of the gremlin-python
Gremlin Language Variant (GLV) that is bundled with Apache TinkerPop beginning
with the 3.2.2 release. Traversal are generated using the class
:py:class:`AsyncGraph<goblin.driver.graph.AsyncGraph>` combined with a remote
connection class, either :py:class:`Connection<goblin.driver.connection.Connection>` or
:py:class:`DriverRemoteConnection<goblin.driver.connection.DriverRemoteConnection>`::

    >>> import asyncio
    >>> from goblin import driver

    >>> loop = asyncio.get_event_loop()
    >>> remote_conn = loop.run_until_complete(
    ...     driver.Connection.open(
    ...         "http://localhost:8182/gremlin", loop))
    >>> graph = driver.AsyncGraph()
    >>> g = graph.traversal().withRemote(remote_conn)

Once you have a traversal source, it's all Gremlin...::

    >>> traversal = g.addV('query_language').property('name', 'gremlin')

`traversal` is in an instance of
:py:class:`AsyncGraphTraversal<goblin.driver.graph.AsyncGraphTraversal>`, which
implements the Python 3.5 asynchronous iterator protocol::

    >>> async def iterate_traversal(traversal):
    >>>     async for msg in traversal:
    >>>         print(msg)

    >>> loop.run_until_complete(iterate_traversal(traversal))
    # v[0]

:py:class:`AsyncGraphTraversal<goblin.driver.graph.AsyncGraphTraversal>` also
provides several convenience methods to help iterate over results:

- :py:meth:`next<goblin.driver.graph.AsyncGraphTraversal.next>`
- :py:meth:`toList<goblin.driver.graph.AsyncGraphTraversal.toList>`
- :py:meth:`toSet<goblin.driver.graph.AsyncGraphTraversal.toSet>`
- :py:meth:`oneOrNone<goblin.driver.graph.AsyncGraphTraversal.oneOrNone>`

Notice the mixedCase? Not very pythonic? Well no, but it maintains continuity
with the Gremlin query language, and that's what the GLV is all about...

Note: Gremlin steps that are reserved words in Python, like `or`, `in`, use a
a trailing underscore `or_` and `in_`.

The Side Effect Interface
-------------------------

When using TinkerPop 3.2.2+ with the default
:py:class:`GraphSON2MessageSerializer<goblin.driver.serializer.GraphSON2MessageSerializer>`,
:py:mod:`Goblin` provides an asynchronous side effects interface using the
:py:class:`AsyncRemoteTraversalSideEffects<goblin.driver.graph.AsyncRemoteTraversalSideEffects>`
class. This allows side effects to be retrieved after executing the traversal::

    >>> traversal = g.V().aggregate('a')
    >>> results = loop.run_until_complete(traversal.toList())
    >>> print(results)
    # [v[0]]

Calling
:py:meth:`keys<goblin.driver.graph.AsyncRemoteTraversalSideEffects.keys>`
will then return an asynchronous iterator containing all keys for cached
side effects:

    >>> async def get_side_effect_keys(traversal):
    ...     resp = await traversal.side_effects.keys()
    ...     async for key in resp:
    ...         print(key)


    >>> loop.run_until_complete(get_side_effect_keys(traversal))
    # 'a'

Then calling
:py:meth:`get<goblin.driver.graph.AsyncRemoteTraversalSideEffects.get>`
using a valid key will return the cached side effects::

    >>> async def get_side_effects(traversal):
    ...     resp = await traversal.side_effects.get('a')
    ...     async for side_effect in resp:
    ...         print(side_effect)


    >>> loop.run_until_complete(get_side_effects(traversal))
    # v[0]

And that's it! For more information on Gremlin Language Variants, please
visit the `Apache TinkerPop GLV Documentation`_.


.. _Apache TinkerPop GLV Documentation: http://tinkerpop.apache.org/docs/3.2.2/tutorials/gremlin-language-variants/
