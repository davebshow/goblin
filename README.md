## Why davebshow/goblin???

Developers note:
The original Goblin was a TinkerPop 3 ready port of Cody Lee's mogwai, an excellent library that had been developed for use with pre-TinkerPop 3 versions of Titan. We designed Goblin to provide asynchronous programming abstractions that would work using any version of Python 2.7 + with a variety of asynchronous I/O libraries (Tornado, Asyncio, Trollius). While in theory this was great, we found that in our effort to promote compatibility we lost out on many of the features the newer Python versions provide to help developers deal with asynchronous programming. Our code base became large and made heavy use of callbacks, and nearly all methods and functions returned some sort of `Future`. This created both a clunky user API, and a code base that was difficult to reason about and maintain.

So, we decided to rewrite Goblin from scratch...

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
