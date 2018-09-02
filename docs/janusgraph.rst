Using Goblin with JanusGraph
============================

JanusGraph 0.2.0 only supports TinkerPop 3.2.6. The `aiogremlin`/`gremlinpython` version
must match this to interface with JanusGraph. By default the newest version of the driver
found on PyPi is installed to downgrade::

    >>> pip uninstall gremlinpython
    >>> pip uninstall aiogremlin
    >>> pip uninstall tornado

    >>> pip install gremlinpython==3.2.6 --no-deps
    >>> pip install aiogremlin==3.2.6

Interfacing `goblin` to JanusGraph requires some simple but non-obvious configuration::

    >>> import asyncio
    >>> import goblin

    >>> def get_hashable_id(val):
    ...     if val["@type"] == "janusgraph:RelationIdentifier":
    ...         val = val["@value"]["value"]
    ...
    ...     return val

    >>> loop = asyncio.get_event_loop()
    >>> app = loop.run_until_complate(
    ...     goblin.Goblin.open(loop, get_hashable_id=get_hashable_id))

Creating custom schema
----------------------

Passing custom schema to JanusGraph using the cluster client. This example creates a
property with a `java.util.Date` type to serialize a Python datetime value::

    >>> import asyncio
    >>> import datetime
    >>> import goblin
    >>> from aiogremlin import Cluster
    >>> from aiogremlin.gremlin_python.driver import serializer
    >>> from aiogremlin.gremlin_python.process.traversal import P
    >>> from aiogremlin.gremlin_python.structure.io import graphson

    >>> class DateSerializer:
    ...     def dictify(self, obj, writer):
    ...        # Java timestamp expects miliseconds
    ...        ts = round(obj.timestamp() * 1000)
    ...        return graphson.GraphSONUtil.typedValue('Date', ts)

    >>> class DateDeserializer:
    ...     def objectify(self, ts, reader):
    ...         # Python timestamp expects seconds
    ...         dt = datetime.datetime.fromtimestamp(ts / 1000.0)
    ...         return dt

    >>> reader = graphson.GraphSONReader({'g:Date': DateDeserializer()})
    >>> writer = graphson.GraphSONWriter({datetime.datetime: DateSerializer()})

    >>> message_serializer = serializer.GraphSONMessageSerializer(reader=reader, writer=writer)

    >>> loop = asyncio.get_event_loop()
    >>> cluster = loop.run_until_complete(
    ...     Cluster.open(loop, message_serializer=message_serializer))
    >>> app = goblin.Goblin(cluster)

    >>> class DateTime(goblin.abc.DateType):
    ...     def validate(self, val):
    ...         if not isinstance(val, datetime.datetime):
    ...             raise goblin.exception.ValidationError(
    ...                 "Not a valid datetime.datetime: {}".format(val))
    ...         return val
    ...
    ...     def to_ogm(self, val):
    ...         return super().to_ogm(val)
    ...
    ...     def to_db(self, val):
    ...         return super.to_db(val)

    >>> class Event(goblin.Vertex):
    ...     name = goblin.Property(goblin.String)
    ...     datetime = goblin.Property(DateTime)

    >>> app.register(Event)

    >>> async def create_schema():
    ...     client = await cluster.connect()
    ...     schma_msg = """mgmt = graph.openManagement()
    ...                    datetime = mgmt.makePropertyKey('datetime').dataType(Date.class).cardinality(Cardinality.SINGLE).make()
    ...                    mgmt.commit()"""
    ...     await client.submit(schema_msg)

    >>> async def go():
    ...     session = await app.session()
    ...     # Create an event with a datetime property
    ...     event1 = Event()
    ...     event1.name = 'event1'
    ...     event1.datetime = datetime.datetime.now()
    ...     # Get a timestamp for comparisons
    ...     await asyncio.sleep(0.001)
    ...     ts = datetime.datetime.now()
    ...     await asyncio.sleep(0.001)
    ...     # Create an event with a later datetime attribute
    ...     event2 = Event()
    ...     event2.name = 'event2'
    ...     event2.datetime = datetime.datetime.now()
    ...
    ...     # Add event verts to DB
    ...     session.add(event1, event2)
    ...     await session.flush()
    ...
    ...     # Query based on datetime
    ...     earlier_event = await session.g.V().has('datetime', P.lt(ts)).next()
    ...     print("{} occured at {}".format(earlier_event.name, earlier_event.datetime))
    ...
    ...     later_event = await session.g.V().has('datetime', P.gt(ts)).next()
    ...     print("{} occured at {}".format(later_event.name, later_event.datetime))
    ...
    ... loop.run_until_complete(create_schema())
    ... loop.run_until_complete(go())
    ... loop.run_until_complete(app.close())
