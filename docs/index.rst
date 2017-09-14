.. goblin documentation master file, created by
   sphinx-quickstart on Sat Jul 16 14:01:32 2016.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Goblin - Async Python toolkit for the TinkerPop 3 Gremlin Server
================================================================

:py:mod:`Goblin` is an asynchronous Python toolkit for the `TinkerPop 3`_
`Gremlin Server`_. In order to leverage Python's support for asynchronous
programming paradigms, :py:mod:`Goblin<goblin>` is implemented using the async/await
syntax introduced in Python 3.5, and does not support earlier Python versions. Goblin
is built on top of `aiogremlin`_ and provides full compatibility with the `aiogremlin`_
GLV and driver.

**Main features**:

- High level asynchronous *Object Graph Mapper* (OGM)

- Integration with the *official gremlin-python Gremlin Language Variant* (GLV) - now
  provided by `aiogremlin`_

- Native Python support for asynchronous programing including *coroutines*,
  *iterators*, and *context managers* as specified in `PEP 492`_

- *Asynchronous Python driver* for the `Gremlin Server`_ - now
  provided by `aiogremlin`_

Releases
========
The latest release of :py:mod:`goblin` is **2.0.0**.


Requirements
============

- Python 3.5+
- TinkerPop 3.2.4


Dependencies
============
- aiogremlin 3.2.4
- inflection 0.3.1

Installation
============
Install using pip::

    $ pip install goblin


The Basics
----------

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
    ...     result = await session.g.E(works_with.id).next()
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

**Gremlin Language Variant**

Generate and submit Gremlin traversals in native Python::

    >>> from goblin import DriverRemoteConnection  # alias for aiogremlin.DriverRemoteConnection
    >>> from goblin import Graph  # alias for aiogremlin.Graph

    >>> async def go(loop):
    ...    remote_connection = await DriverRemoteConnection.open(
    ...        'ws://localhost:8182/gremlin', 'g')
    ...    g = Graph().traversal().withRemote(remote_connection)
    ...    vertices = await g.V().toList()
    ...    await remote_connection.close()
    ...    return vertices

    >>> results = loop.run_until_complete(go(loop))
    >>> results
    # [v[1], v[2], v[3], v[4], v[5], v[6]]


    >>> loop.run_until_complete(go(g))
    # {'properties': {'name': [{'value': 'Leif', 'id': 3}]}, 'label': 'developer', 'id': 2, 'type': 'vertex'}

For more information on using the :py:class:`Graph<aiogremlin.structure.graph.Graph>`,
see the `aiogremlin`_ documentation or the :doc:`GLV docs</glv>`



**Driver**

Submit scripts and bindings to the `Gremlin Server`_::

    >>> import asyncio
    >>> from goblin import Cluster  # alias for aiogremlin.Cluster

    >>> loop = asyncio.get_event_loop()

    >>> async def go(loop):
    ...     cluster = await Cluster.open('ws://localhost:8182/gremlin', loop)
    ...     client = await cluster.connect()
    ...     resp = await client.submit(
    ...         "g.addV('developer').property(k1, v1)",
    ...         bindings={'k1': 'name', 'v1': 'Leif'})
    ...         async for msg in resp:
    ...             print(msg)
    ...      await cluster.close()

    >>> loop.run_until_complete(go(loop))

For more information on using the driver, see the `aiogremlin`_ documentation or the :doc:`Driver docs</driver>`


Contents:

.. toctree::
   :maxdepth: 4

   ogm
   glv
   driver
   app
   performance
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
.. _Github: https://github.com/davebshow/goblin/issues
.. _PEP 492: https://www.python.org/dev/peps/pep-0492/
.. _aiogremlin: http://aiogremlin.readthedocs.io/en/latest/
