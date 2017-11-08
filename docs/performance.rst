Improving Driver Performance
============================


The :py:mod:`goblin.driver` aims to be as performant as possible, yet it is
potentially limited by implementation details, as well as its underlying
software stack i.e., the websocket client, the event loop implementation, etc.
If necessary, a few tricks can boost its performance.


Use ``cython``
--------------

Before installing :py:mod:`Goblin<goblin>`, install ``cython``::

    $ pip install cython


Use ``ujson``
-------------

Install ``ujson`` to speed up serialzation::

    $ pip install ujson


Use ``uvloop``
--------------

Install ``uvloop``, a Cython implementation of an event loop::

    $ pip install uvloop

Then, in application code, set the :py:func:`asyncio.set_event_loop_policy`::

    >>> import asyncio
    >>> import uvloop
    >>> asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
