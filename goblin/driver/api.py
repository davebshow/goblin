import asyncio
import aiohttp

from goblin.driver import pool
from goblin.driver import connection


class GremlinServer:

    @classmethod
    async def open(cls,
                   url: str,
                   loop: asyncio.BaseEventLoop,
                   *,
                   conn_factory: aiohttp.ClientSession=None,
                   max_inflight: int=None,
                   force_close: bool=False,
                   force_release: bool=False,
                   pool: pool.Pool=None,
                   username: str=None,
                   password: str=None) -> connection.Connection:
        # Use connection factory here
        if conn_factory is None:
            conn_factory = aiohttp.ClientSession(loop=loop)
            ws = await conn_factory.ws_connect(url)
        return connection.Connection(ws, loop, conn_factory,
                                     max_inflight=max_inflight,
                                     force_close=force_close,
                                     force_release=force_release,
                                     pool=pool, username=username,
                                     password=password)

    @classmethod
    async def create_client(cls,
                            url: str,
                            loop: asyncio.BaseEventLoop,
                            *,
                            conn_factory: aiohttp.ClientSession=None,
                            max_inflight: int=None,
                            max_connections: int=None,
                            force_close: bool=False,
                            force_release: bool=False,
                            pool: pool.Pool=None,
                            username: str=None,
                            password: str=None) -> connection.Connection:
        pass
