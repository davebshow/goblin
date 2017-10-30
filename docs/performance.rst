Improving Driver Performance
============================


The :py:mod:`goblin.driver` aims to be as performant as possible, yet it is
potentially limited by implementation details, as well as its underlying
software stack i.e., the websocket client, the event loop implementation, etc.
If necessary, a few tricks can boost its performance.


Use :py:mod:`Cython`
--------------------

Before installing :py:mod:`Goblin`, install :py:mod:`Cython`::

    $ pip install cython


Use :py:mod:`ujson`
-------------------

Install :py:mod:`ujson` to speed up serialzation::

    $ pip install ujson


Use :py:mod:`uvloop`
--------------------

Install :py:mod:`uvloop`, a Cython implementation of an event loop::

    $ pip install uvloop

Then, in application code, set the :py:func:`asyncio.event_loop_policy`::

    >>> import asyncio
    >>> import uvloop
    >>> asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
