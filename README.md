## Why davebshow/goblin???

Let's face it, ZEROFAIL/goblin has problems. It's not really anyone's fault, it is just a fact of life. The mogwai codebase is monumental and it makes heavy use of metaprogramming, instrumentation, class inheritance, global variables and configuration, and third-party libraries. This results in opaque code that is hard to read and maintain, resulting in less community interest and more work for developers looking to make improvements. My port to TinkerPop3 just made things worse, as it introduced a level of callback hell only know to the most hardcore JavaScript developers. At this point, while ZEROFAIL/goblin basically works, implementing new functionality is a daunting task, and simple debugging and reasoning have become serious work. I was trying to think how to remedy these problems and rescue goblin from an ugly, bloated fate as we move forward adding new functionality. Then, it dawned on me: all I need to do is drag that whole folder to the virtual trash bin on my desktop and start from scratch.

But, wait, a whole new OGM from scratch...? Well, yes and no. Borrowing a few concepts from SQLAlchemy and using what I've learned from working on previous Python software targeting the Gremlin Server, I was able to piece together a fully functioning system in just a few hours of work and less than 1/10 the code. So, here is my *community prototype* OGM contribution to the Python TinkerPop3 ecosystem.

## Features

1. *Transparent* Python 3.5 codebase using PEP 492 await/async syntax that leverages asynchronous iterators and asynchronous context managers
2. SQLAlchemy style element creation and queries
3. Full integration with the official GLV gremlin-python
4. Transparent mapping between OGM and database values
5. Support for session/transaction management
6. Graph database vendor agnostic/configurable
7. Fully extensible data type system
8. Descriptor based property assignment
9. And more...!

### Install
```
$ pip install git+https://github.com/davebshow/goblin.git
```
### Create/update elements

```python

import asyncio

from goblin.api import create_engine, Vertex, Edge
from goblin.properties import Property, String


class TestVertex(Vertex):
    __label__ = 'test_vertex'
    name = Property(String)
    notes = Property(String, initval='N/A')


class TestEdge(Edge):
    __label__ = 'test_edge'
    notes = Property(String, initval='N/A')


loop = asyncio.get_event_loop()
engine = loop.run_until_complete(
    create_engine("http://localhost:8182/", loop))


async def create():
    session = engine.session()
    leif = TestVertex()
    leif.name = 'leifur'
    jon = TestVertex()
    jon.name = 'jonathan'
    works_for = TestEdge()
    works_for.source = jon
    works_for.target = leif
    assert works_for.notes == 'N/A'
    works_for.notes = 'zerofail'
    session.add(leif, jon, works_for)
    await session.flush()
    print(leif.name, leif.id, jon.name, jon.id,
          works_for.notes, works_for.id)
    leif.name = 'leif'
    session.add(leif)
    await session.flush()
    print(leif.name, leif.id)


loop.run_until_complete(create())
# leifur 0 jonathan 3 zerofail 6
# leif 0
```

### Query the db:

```python
async def query():
    session = engine.session()
    stream = await session.query(TestVertex).all()
    async for msg in stream:
        print(msg)


loop.run_until_complete(query())
# [<__main__.TestVertex object at 0x7f46d833e588>, <__main__.TestVertex object at 0x7f46d833e780>]

```

### See how objects map to the db:

```python
TestVertex.__mapping__
# <Mapping(type=vertex, label=test_vertex, properties=[
  # {'db_name': 'test_vertex__name', 'ogm_name': 'name', 'data_type': <class 'goblin.properties.String'>},
  # {'db_name': 'test_vertex__notes', 'ogm_name': 'notes', 'data_type': <class 'goblin.properties.String'>}])

```

### Close the engine

```python
loop.run_until_complete(engine.close())
```
