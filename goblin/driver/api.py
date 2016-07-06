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
                   client_session: aiohttp.ClientSession=None,
                   force_close: bool=False,
                   username: str=None,
                   password: str=None) -> connection.Connection:
        if client_session is None:
            client_session = aiohttp.ClientSession(loop=loop)
        ws = await client_session.ws_connect(url)
        return connection.Connection(ws, loop, client_session,
                                     force_close=force_close,
                                     username=username, password=password)
