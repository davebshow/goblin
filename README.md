[![Build Status](https://travis-ci.org/ZEROFAIL/goblin.svg?branch=master)](https://travis-ci.org/ZEROFAIL/goblin)

[![Coverage Status](https://coveralls.io/repos/github/ZEROFAIL/goblin/badge.svg?branch=master)](https://coveralls.io/github/ZEROFAIL/goblin?branch=master)

# Goblin

[Official Documentation](http://goblin.readthedocs.io/en/latest/)


Developers note:
The original Goblin was a TinkerPop 3 ready port of Cody Lee's mogwai, an excellent library that had been developed for use with pre-TinkerPop 3 versions of Titan. We designed Goblin to provide asynchronous programming abstractions that would work using any version of Python 2.7 + with a variety of asynchronous I/O libraries (Tornado, Asyncio, Trollius). While in theory this was great, we found that in our effort to promote compatibility we lost out on many of the features the newer Python versions provide to help developers deal with asynchronous programming. Our code base became large and made heavy use of callbacks, and nearly all methods and functions returned some sort of `Future`. This created both a clunky user API, and a code base that was difficult to reason about and maintain.

So, we decided to rewrite Goblin from scratch...

## Features

- Integration with the *official gremlin-python Gremlin Language Variant* (GLV)

- Native Python support for asynchronous programing including *coroutines*,
  *iterators*, and *context managers* as specified in PEP 492

- *Asynchronous Python driver* for the Gremlin Server

- `AsyncRemoteGraph` implementation that produces *native Python GLV traversals*

- High level asynchronous *Object Graph Mapper* (OGM)
