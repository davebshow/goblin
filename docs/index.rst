.. goblin documentation master file, created by
   sphinx-quickstart on Sat Jul 16 14:01:32 2016.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Goblin - Async Python toolkit for the TinkerPop 3 Gremlin Server
================================================================

:py:mod:`Goblin` is an asynchronous Python toolkit for the `TinkerPop 3`_
`Gremlin Server`_. In order to leverage Python's support for asynchronous
programming paradigms, :py:mod:`Goblin<goblin>` is implemented using the async/await
syntax introduced in Python 3.5, and does not support earlier Python versions.

**Main features**:

- Integration with the *official gremlin-python Gremlin Language Variant* (GLV)

- Native Python support for asynchronous programing including *coroutines*,
  *iterators*, and *context managers* as specified in `PEP 492`_

- *Asynchronous Python driver* for the `Gremlin Server`_

- :py:class:`AsyncRemoteGraph<goblin.driver.graph.AsyncRemoteGraph>`
  implementation that produces *native Python GLV traversals*

- High level asynchronous *Object Graph Mapper* (OGM)

The Basics
----------

Install using pip::

    $ pip install goblin

**Driver**

Submit scripts and bindings to the `Gremlin Server`_::

    >>> import asyncio
    >>> from goblin import driver


    >>> loop = asyncio.get_event_loop()


    >>> async def go(loop):
    ...     script = "g.addV('developer').property(k1, v1)"
    ...     bindings = {'k1': 'name', 'v1': 'Leif'}
    ...     conn = await driver.Connection.open(
    ...         'ws://localhost:8182/gremlin', loop)
    ...     async with conn:
    ...         resp = await conn.submit(gremlin=script, bindings=bindings)
    ...         async for msg in resp:
    ...             print(msg)


    >>> loop.run_until_complete(go(loop))
    # {'type': 'vertex', 'id': 0, 'label': 'developer', 'properties': {'name': [{'id': 1, 'value': 'Leif'}]}}

For more information on using the driver, see the :doc:`Driver docs</driver>`

**AsyncGraph**

Generate and submit Gremlin traversals in native Python::


    >>> remote_conn = loop.run_until_complete(
    ...     driver.Connection.open(
    ...         "http://localhost:8182/gremlin", loop))
    >>> graph = driver.AsyncGraph()
    >>> g = graph.traversal().withRemote(remote_conn)


    >>> async def go(g):
    ...     traversal = g.addV('developer').property('name', 'Leif')
    ...     async for msg in traversal:
    ...         print(msg)
    ...     await remote_conn.close()


    >>> loop.run_until_complete(go(g))
    # {'properties': {'name': [{'value': 'Leif', 'id': 3}]}, 'label': 'developer', 'id': 2, 'type': 'vertex'}

For more information on using the :py:class:`goblin.driver.graph.AsyncGraph<AsyncGraph>`,
see the :doc:`GLV docs</glv>`


**OGM**

Define custom vertex/edge classes using the provided base :py:mod:`classes<goblin.element>`,
:py:class:`properties<goblin.properties.Property>`, and
:py:mod:`data types<goblin.properties>`::

    >>> from goblin import element, properties


    >>> class Person(element.Vertex):
    ...     name = properties.Property(properties.String)
    ...     age = properties.Property(properties.Integer)


    >>> class Knows(element.Edge):
    ...     notes = properties.Property(properties.String, default='N/A')


Create a :py:class:`Goblin App<goblin.app.Goblin>` and register the element classes::

    >>> from goblin import Goblin


    >>> app = loop.run_until_complete(
    ...     Goblin.open(loop))
    >>> app.register(Person, Knows)


Other than user defined properties, elements provide no interface. Use a
:py:class:`Session<goblin.session.Session>` object to interact with the
database::

    >>> async def go(app):
    ...     session = await app.session()
    ...     leif = Person()
    ...     leif.name = 'Leif'
    ...     leif.age = 28
    ...     jon = Person()
    ...     jon.name = 'Jonathan'
    ...     works_with = Knows(leif, jon)
    ...     session.add(leif, jon, works_with)
    ...     await session.flush()
    ...     result = await session.g.E(works_with.id).oneOrNone()
    ...     assert result is works_with
    ...     people = session.traversal(Person)  # element class based traversal source
    ...     async for person in people:
    ...         print(person)


    >>> loop.run_until_complete(go(app))
    # <__main__.Person object at 0x7fba0b7fa6a0>
    # <__main__.Person object at 0x7fba0b7fae48>

Note that a :py:mod:`Goblin` session does not necessarily correspond to a Gremlin Server session.
Instead, all elements created using a session are 'live' in the sense that if the
results of a traversal executed against the session result in different property values
for an element, that element will be updated to reflect these changes.

For more information on using the OGM, see the :doc:`OGM docs</ogm>`


A note about GraphSON message serialization
-------------------------------------------

The :py:mod:`goblin.driver` provides support for both GraphSON2 and GraphSON1
out of the box. By default, it uses the
:py:class:`GraphSON2MessageSerializer<goblin.driver.serializer.GraphSON2MessageSerializer>`.
Since GraphSON2 was only recently included in the TinkerPop 3.2.2 release,
:py:mod:`goblin.driver` also ships with
:py:class:`GraphSONMessageSerializer<goblin.driver.serializer.GraphSONMessageSerializer>`.
In the near future (when projects like Titan and DSE support the 3.2 Gremlin
Server line), support for GraphsSON1 will be dropped.

The :py:mod:`goblin<Goblin>` OGM still uses GraphSON1 by default and will do so
until :py:mod:`goblin.driver` support is dropped. It will then be updated to
use GraphSON2.


Contents:

.. toctree::
   :maxdepth: 4

   ogm
   glv
   driver
   modules



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

.. _Tinkerpop 3: http://tinkerpop.incubator.apache.org/
.. _Gremlin Server: http://tinkerpop.apache.org/docs/3.1.1-incubating/reference/#gremlin-server
.. _`Asyncio`: https://docs.python.org/3/library/asyncio.html
.. _`aiohttp`: http://aiohttp.readthedocs.org/en/stable/
.. _Github: https://github.com/ZEROFAIL/goblin/issues
.. _PEP 492: https://www.python.org/dev/peps/pep-0492/
