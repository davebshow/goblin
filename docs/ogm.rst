Using the OGM
=============

:py:mod:`Goblin` aims to provide a powerful Object Graph Mapper **(OGM)** while maintaining
a simple, transparent interface. This document describes the OGM components in
more detail.

Modeling Graph Elements with :py:mod:`Goblin`
---------------------------------------------

At the core of the :py:mod:`Goblin` is the concept of the graph element. TinkerPop 3 (TP3)
uses three basic kinds of elements: Vertex, Edge, and Property. In order to achieve
consistent mapping between Python objects and TP3 elements, :py:mod:`Goblin` provides
three corresponding Python base classes that are used to model graph data:
:py:class:`Vertex<goblin.element.Vertex>`, :py:class:`Edge<goblin.element.Edge>`, and
:py:class:`Property<goblin.properties.Property>`. While these classes are created to interact
smoothly with TP3, it is important to remember that :py:mod:`Goblin` does not attempt
to implement the same element interface found in TP3. Indeed, other than user defined
properties, :py:mod:`Goblin` elements feature little to no interface. To begin
modeling data, simply create *model* element classes that inherit from the
:py:mod:`goblin.element` classes. For example::

    import goblin
    from aiogremlin.gremlin_python import Cardinality


    class Person(goblin.Vertex):
        pass


    class City(goblin.Vertex):
        pass


    class BornIn(goblin.Edge):
        pass


And that is it, these are valid element classes that can be saved to the graph
database. Using these three classes we can model a series of people that are connected
to the cities in which they were born. However, these elements
aren't very useful, as they don't contain any information about the person or place. To remedy
this, add some properties to the classes.

Using :py:mod:`goblin.properties`
---------------------------------

Using the :py:mod:`properties<goblin.properties>` module is a bit more involved,
but it is still pretty easy. It simply requires that you create properties that
are defined as Python class attributes, and each property requires that you pass
a :py:class:`DataType<goblin.abc.DataType>` class **or** instance as the first
positional argument. This data type, which is a concrete class that inherits from
:py:class:`DataType<goblin.abc.DataType>`, handles validation, as well as any necessary
conversion when data is mapped between the database and the OGM. :py:mod:`Goblin`
currently ships with 4 data types: :py:class:`String<goblin.properties.String>`,
:py:class:`Integer<goblin.properties.Integer>`,
:py:class:`Float<goblin.properties.Float>`, and
:py:class:`Boolean<goblin.properties.Boolean>`. Example property definition::


    import goblin


    class Person(goblin.Vertex):
        name = goblin.Property(goblin.String)


    class City(goblin.Vertex):
        name = goblin.Property(goblin.String)
        population = goblin.Property(goblin.Integer)


    class BornIn(goblin.Edge):
        pass


:py:mod:`Goblin` :py:mod:`properties<goblin.properties.Property>` can also
be created with a default value, set by using the kwarg `default` in the class
definition::


    class BornIn(goblin.Edge):
        date = goblin.Property(goblin.String, default='unknown')


Creating Elements and Setting Property Values
---------------------------------------------

Behind the scenes, a small metaclass (the only metaclass used in :py:mod:`Goblin`),
substitutes a :py:class:`PropertyDescriptor<goblin.properties.PropertyDescriptor>`
for the :py:class:`Property<goblin.properties.Property>`, which provides a simple
interface for defining and updating properties using Python's descriptor protocol::

    >>> leif = Person()
    >>> leif.name = 'Leif'

    >>> detroit = City()
    >>> detroit.name = 'Detroit'
    >>> detroit.population = 	5311449  # CSA population

    # change a property value
    >>> leif.name = 'Leifur'

In the case that an invalid property value is set, the validator will raise
a :py:class:`ValidationError<goblin.exception.ValidationError>` immediately::


  >>> detroit.population = 'a lot of people'
  ValidationError: Not a valid integer: a lot of people


Creating Edges
--------------
Creating edges is very similar to creating vertices, except that edges require
that a source (outV) and target (inV) vertex be specified. Both source and
target nodes must be :py:mod:`Goblin vertices<goblin.element.Vertex>`. Furthermore,
they must be created in the database before the edge. This is further discussed
below in the :ref:`Session<session>` section. Source and target vertices may be
passed to the edge on instantiation, or added using the property interface::

    >>> leif_born_in_detroit = BornIn(leif, detroit)
    # or
    >>> leif_born_in_detroit = BornIn()
    >>> leif_born_in_detroit.source = leif
    >>> leif_born_in_detroit.target = detroit
    >>> leif_born_in_detroit.date  # default value
    'unknown'


Vertex Properties
-----------------

In addition to the aforementioned elements, TP3 graphs also use a special kind
of property, called a vertex property, that allows for list/set cardinality and
meta-properties. To accommodate this, :py:mod:`Goblin` provides a class
:py:class:`VertexProperty<goblin.element.VertexProperty>` that can be used directly
to create multi-cardinality properties::

    class Person(goblin.Vertex):
        name = goblin.Property(goblin.String)
        nicknames = goblin.VertexProperty(
            goblin.String, card=Cardinality.list)


    >>> david = Person()
    >>> david.name = 'David'
    >>> david.nicknames = ['Dave', 'davebshow']


Notice that the cardinality of the
:py:class:`VertexProperty<goblin.element.VertexProperty>` must be explicitly
set using the `card` kwarg and the
:py:class:`Cardinality<aiogremlin.gremlin_python.Cardinality>` enumerator.

:py:class:`VertexProperty<goblin.element.VertexProperty>` provides a different
interface than the simple, key/value style
:py:class:`PropertyDescriptor<goblin.properties.PropertyDescriptor>` in order to
accomodate more advanced functionality. For accessing multi-cardinality
vertex properties, :py:mod:`Goblin` provides several helper classes called
:py:mod:`managers<goblin.manager>`. The
:py:class:`managers<goblin.manager.ListVertexPropertyManager>` inherits from
:py:class:`list` or :py:class:`set` (depending on the specified cardinality),
and provide a simple API for accessing and appending vertex properties. To continue
with the previous example, we see the `dave` element's nicknames::

    >>> david.nicknames
    [<VertexProperty(type=<goblin.properties.String object at 0x7f87a67a3048>, value=Dave),
     <VertexProperty(type=<goblin.properties.String object at 0x7f87a67a3048>, value=davebshow)]

To add a nickname without replacing the earlier values, we simple :py:meth:`append` as
if the manager were a Python :py:class:`list`::

    >>> david.nicknames.append('db')
    >>> david.nicknames
    [<VertexProperty(type=<goblin.properties.String object at 0x7f87a67a3048>, value=Dave),
     <VertexProperty(type=<goblin.properties.String object at 0x7f87a67a3048>, value=davebshow),
     <VertexProperty(type=<goblin.properties.String object at 0x7f87a67a3048>, value=db)]

If this were a :py:class:`VertexProperty<goblin.element.VertexProperty>` with
a set cardinality, we would simply use :py:meth:`add` to achieve similar functionality.

Both :py:class:`ListVertexPropertyManager<goblin.manager.ListVertexPropertyManager>` and
:py:class:`SetVertexPropertyManager<goblin.manager.SetVertexPropertyManager>` provide a simple
way to access a specific :py:class:`VertexProperty<goblin.element.VertexProperty>`.
You simply call the manager, passing the value of the vertex property to be accessed:

    >>> db = dave.nicknames('davebshow')
    <VertexProperty(type=<goblin.properties.String object at 0x7f87a67a3048>, value=davebshow)

The value of the vertex property can be accessed using the `value` property::

    >>> db.value
    'davebshow'


Meta-properties
---------------

:py:class:`VertexProperty<goblin.element.VertexProperty>` can also be used as
a base classes for user defined vertex properties that contain meta-properties.
To create meta-properties, define a custom vertex property class just like you
would any other element, adding as many simple (non-vertex) properties as needed::

    class HistoricalName(goblin.VertexProperty):
        notes = goblin.Property(goblin.String)

Now, the custom :py:class:`VertexProperty<goblin.element.VertexProperty>` can be added to a
vertex class, using any cardinality::

    class City(goblin.Vertex):
        name = goblin.Property(goblin.String)
        population = goblin.Property(goblin.Integer)
        historical_name = HistoricalName(
            goblin.String, card=Cardinality.list)

Now, meta-properties can be set on the :py:class:`VertexProperty<goblin.element.VertexProperty>`
using the descriptor protocol::

    >>> montreal = City()
    >>> montreal.historical_name = ['Ville-Marie']
    >>> montreal.historical_name('Ville-Marie').notes = 'Changed in 1705'

And that's it.

.. _session:

Saving Elements to the Database Using :py:class:`Session<goblin.session.Session>`
---------------------------------------------------------------------------------

All interaction with the database is achieved using the
:py:class:`Session<goblin.session.Session>` object. A :py:mod:`Goblin` session
should not be confused with a Gremlin Server session, although in future releases
it will provide support for server sessions and transactions. Instead,
the :py:class:`Session<goblin.session.Session>` object is used to save elements
and spawn Gremlin traversals. Furthemore, any element created using a session is
*live* in the sense that a :py:class:`Session<goblin.session.Session>` object
maintains a reference to session elements, and if a traversal executed using a
session returns different property values for a session element, these values are
automatically updated on the session element. Note - the examples shown in this section
must be wrapped in coroutines and ran using the :py:class:`asyncio.BaseEventLoop`,
but, for convenience, they are shown as if they were run in a Python interpreter.
To use a :py:class:`Session<goblin.session.Session>`, first create a
:py:class:`Goblin App <goblin.app.Goblin>` using
:py:meth:`Goblin.open<goblin.app.Goblin.open>`, then register the defined element
classes::

    >>> app = await goblin.Goblin.open(loop)
    >>> app.register(Person, City, BornIn)
    >>> session = await app.session()

Goblin application support a variety of configuration options, for more information
see :doc:`the Goblin application documentation</app>`.

The best way to create elements is by adding them to the session, and then flushing
the `pending` queue, thereby creating the elements in the database. The order in which
elements are added **is** important, as elements will be created based on the order
in which they are added. Therefore, when creating edges, it is important to add the
source and target nodes before the edge (if they don't already exits). Using
the previously created elements::

    >>> session.add(leif, detroit, leif_born_in_detroit)
    >>> await session.flush()

And that is it. To see that these elements have actually been created in the db,
check that they now have unique ids assigned to them::

    >>> assert leif.id
    >>> assert detroit.id
    >>> assert leif_born_in_detroit.id

For more information on the :py:class:`Goblin App <goblin.app.Goblin>`, please
see :doc:`Using the Goblin App</app>`

:py:class:`Session<goblin.session.Session>` provides a variety of other CRUD functions,
but all creation and updating can be achieved simply using the :py:meth:`add` and
:py:meth:`flush` methods.


Writing Custom Gremlin Traversals
---------------------------------

Finally, :py:class:`Session<goblin.session.Session>` objects allow you to write
custom Gremlin traversals using the official gremlin-python Gremlin Language Variant
**(GLV)**. There are two methods available for writing session based traversals. The first,
:py:meth:`traversal<goblin.session.Session.traversal>`, accepts an element class as a
positional argument. This is merely for convenience, and generates this equivalent
Gremlin::

    >>> session.traversal(Person)
    g.V().hasLabel('person')

Or, simply use the property :py:attr:`g<goblin.session.Session.g>`::

    >>> session.g.V().hasLabel('person')...


In general property names are mapped directly from the OGM to the database.
However, by passing the `db_name` kwarg to a property definition, the user has
the ability to override this behavior. To avoid mistakes in the case of custom
database property names, it is encouraged to access the mapped property names
as class attributes::

    >>> Person.name
    'name'

So, to write a traversal::

    >>> session.traversal(Person).has(Person.name, 'Leifur')


Also, it is important to note that certain data types could be transformed
before they are written to the database. Therefore, the data type method `to_db`
may be required::

    >>> session.traversal(Person).has(
    ...     Person.name, goblin.String.to_db('Leifur'))

While this is not the case with any of the simple data types shipped with :py:mod:`Goblin`,
custom data types or future additions may require this kind of operation. Because of
this, :py:mod:`Goblin` includes the convenience function
:py:func:`bindprop<goblin.traversal.bindprop>`, which also allows an optional binding for
the value to be specified::

    >>> traversal = session.traversal(Person)
    >>> traversal.has(bindprop(Person, 'name', 'Leifur', binding='v1'))

Finally, there are a variety of ways to to submit a traversal to the server.
First of all, all traversals are themselve asynchronous iterators, and using
them as such will cause a traversal to be sent on the wire:

    >>> async for msg in session.g.V().hasLabel('person'):
    ...     print(msg)

Furthermore, :py:mod:`Goblin` provides several convenience methods that
submit a traversal as well as process the results :py:meth:`toList`,
:py:meth:`toSet` and :py:meth:`next`. These methods both submit a script
to the server and iterate over the results. Remember to `await` the traversal
when calling these methods::

    >>> traversal = session.traversal(Person)
    >>> leif = await traversal.has(
    ...     bindprop(Person, 'name', 'Leifur', binding='v1')).next()

And that is pretty much it. We hope you enjoy the :py:mod:`Goblin` OGM.
