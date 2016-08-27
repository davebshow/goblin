Using the Driver
================

At the its simplest, the driver provides the
:py:meth:`open<goblin.driver.connection.Connection.open>` coroutine classmethod,
which returns a :py:class:`Connection<goblin.driver.connection.Connection> to the
Gremlin Server::

    >>> import asyncio
    >>> from goblin import driver
    >>> loop = asyncio.get_event_loop()
    >>> conn = await driver.Connection.open('ws://localhost:8182', loop)

The :py:class:`Connection<goblin.driver.connection.Connection> object can be
used to :py:meth:`submit<goblin.driver.connection.Connection.submit>` and
receive messages.
:py:meth:`submit<goblin.driver.connection.Connection.submit>` returns a
:py:class:`Response<goblin.driver.connection.Response>` object that implements
the PEP 492 asynchronous iterator protocol::

    >>> resp = conn.submit('1 + 1')
    >>> async for msg in resp:
    ...     print(msg)
    >>> await conn.close()  # conn also implements async context manager interface

Connecting to a :py:class:`Cluster<goblin.driver.cluster.Cluster>`
------------------------------------------------------------------

To take advantage of the higher level features of the
:py:mod:`driver<goblin.driver>`, :py:mod:`Goblin` provides the
:py:class:`Cluster<goblin.driver.cluster.Cluster>` object.
:py:class:`Cluster<goblin.driver.cluster.Cluster>` is used to create multi-host
clients that leverage connection pooling and sharing. Its interface is based
on the TinkerPop Java driver::

    >>> cluster = await driver.Cluster.open()  # opens a cluster with default config
    >>> client = await cluster.connect()
    >>> resp = client.submit('1 + 1')  # round robin requests to available hosts
    >>> async for msg in resp:
    ...     print(msg)
    >>> await cluster.close()  # Close all connections to all hosts

And that is it. While the :py:class:`Cluster<goblin.driver.cluster.Cluster>`
is simple to learn and use, it provides a wide variety of configuration options.

Configuring :py:class:`Cluster<goblin.driver.cluster.Cluster>`
--------------------------------------------------------------

Configuration options can be set on
:py:class:`Cluster<goblin.driver.cluster.Cluster>` in one of two ways, either
passed as keyword arguments to
:py:meth:`open<goblin.driver.cluster.Cluster.open>` or stored in a configuration
file, and passed to the :py:meth:`open<goblin.driver.cluster.Cluster.open>`
using the kwarg `configfile`. Configuration files can be either YAML or JSON
format. Currently, :py:class:`Cluster<goblin.driver.cluster.Cluster>`
uses the following configuration:

+-------------------+----------------------------------------------+-------------+
|Key                |Description                                   |Default      |
+==================++==============================================+=============+
|scheme             |URI scheme, typically 'ws' or 'wss' for secure|'ws'         |
|                   |websockets                                    |             |
+-------------------+----------------------------------------------+-------------+
|hosts              |A list of hosts the cluster will connect to   |['localhost']|
+-------------------+----------------------------------------------+-------------+
|port               |The port of the Gremlin Server to connect to, |8182         |
|                   |same for all hosts                            |             |
+-------------------+----------------------------------------------+-------------+
|ssl_certfile       |File containing ssl certificate               |''           |
+-------------------+----------------------------------------------+-------------+
|ssl_keyfile        |File containing ssl key                       |''           |
+-------------------+----------------------------------------------+-------------+
|ssl_password       |File containing password for ssl keyfile      |''           |
+-------------------+----------------------------------------------+-------------+
|username           |Username for Gremlin Server authentication    |''           |
+-------------------+----------------------------------------------+-------------+
|password           |Password for Gremlin Server authentication    |''           |
+-------------------+----------------------------------------------+-------------+
|response_timeout   |Timeout for reading responses from the stream |`None`       |
+-------------------+----------------------------------------------+-------------+
|max_conns          |The maximum number of connections open at any |4            |
|                   |time to this host                             |             |
+-------------------+----------------------------------------------+-------------+
|min_conns          |The minimum number of connection open at any  |1            |
|                   |time to this host                             |             |
+-------------------+----------------------------------------------+-------------+
|max_times_acquired |The maximum number of times a single pool     |16           |
|                   |connection can be acquired and shared         |             |
+-------------------+----------------------------------------------+-------------+
|max_inflight       |The maximum number of unresolved messages     |64           |
|                   |that may be pending on any one connection     |             |
+-------------------+----------------------------------------------+-------------+
