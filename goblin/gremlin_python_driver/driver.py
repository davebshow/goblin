"""Simple Async driver for the TinkerPop3 Gremlin Server"""
import asyncio
import collections
import json
import logging
import uuid

import aiohttp


logger = logging.getLogger(__name__)


Message = collections.namedtuple(
    "Message",
    ["status_code", "data", "message", "metadata"])


class Driver:

    def __init__(self, url, loop, *, client_session=None):
        self._url = url
        self._loop = loop
        if not client_session:
            client_session = aiohttp.ClientSession(loop=self._loop)
        self._client_session = client_session
        self._reclaimed = collections.deque()
        self._driver_condition = asyncio.Condition(loop=loop)
        self._open_connections = 0
        self._connecting = 0
        self._max_connections = 4

    @property
    def driver_condition(self):
        return self._driver_condition

    @property
    def max_connections(self):
        return self._max_connections

    @property
    def total_connections(self):
        return self._connecting + self._open_connections

    def get(self):
        return AsyncDriverConnectionContextManager(self)

    async def submit(self,
                     gremlin,
                     *,
                     bindings=None,
                     lang='gremlin-groovy',
                     aliases=None,
                     op="eval",
                     processor="",
                     session=None,
                     request_id=None):
        pass


    async def connect(self, *, force_close=True, force_reclaim=False):
        async with self.driver_condition:
            conn = await self._get_new_connection(force_close, force_reclaim)
            return conn

    async def _get_new_connection(self, force_close, force_reclaim):
        if self.total_connections <= self._max_connections:
            self._connecting += 1
            try:
                ws = await self._client_session.ws_connect(self._url)
                self._open_connections +=1
                return Connection(ws, self._loop, force_close=force_close,
                                  force_reclaim=force_reclaim, driver=self)
            finally:
                self._connecting -= 1
        else:
            raise RuntimeError("To many connections, try recycling")

    async def recycle(self, *, force_close=False, force_reclaim=True):
        async with self.driver_condition:
            while True:
                if self._reclaimed:
                    while self._reclaimed:
                        conn = self._reclaimed.popleft()
                        if not conn.closed:
                            logger.info("Reusing connection: {}".format(conn))
                            return conn
                        else:
                            self._open_connections -= 1
                            logger.debug(
                                "Discarded closed connection: {}".format(conn))
                elif self.total_connections < self.max_connections:
                    conn = await self._get_new_connection(force_close,
                                                          force_reclaim)
                    logger.info("Acquired new connection: {}".format(conn))
                    return conn
                else:
                    await self.driver_condition.wait()

    async def reclaim(self, conn):
        if self.total_connections <= self.max_connections:
            if conn.closed:
                # conn has been closed
                logger.info(
                    "Released closed connection: {}".format(conn))
                self._open_connections -= 1
                conn = None
            else:
                self._reclaimed.append(conn)
            await self._wakeup()
        else:
            if conn.driver is self:
                # hmmm
                await conn.close()
                self._open_connections -= 1

    async def _wakeup(self):
        async with self.driver_condition:
            self.driver_condition.notify()

    async def close(self):
        async with self.driver_condition:
            waiters = []
            while self._reclaimed:
                conn = self._reclaimed.popleft()
                waiters.append(conn.close())
            await asyncio.gather(*waiters, loop=self._loop)
            await self._client_session.close()
            self._client_session = None
            self._closed = True
            logger.debug("Driver {} has been closed".format(self))


class AsyncDriverConnectionContextManager:

    __slots__ = ('_driver', '_conn')

    def __init__(self, driver):
        self._driver = driver

    async def __aenter__(self):
        self._conn = await self._driver.connect(force_close=False)
        return self._conn

    async def __aexit__(self, exc_type, exc, tb):
        await self._conn.close()
        self._conn = None
        self._driver = None


class AsyncResponseIter:

    def __init__(self, ws, loop, conn, username, password, processor, session):
        self._ws = ws
        self._loop = loop
        self._conn = conn
        self._username = username
        self._password = password
        self._processor = processor
        self._session = session
        self._force_close = self._conn.force_close
        self._force_reclaim = self._conn.force_reclaim
        self._closed = False
        self._response_queue = asyncio.Queue(loop=loop)

    async def __aiter__(self):
        return self

    async def __anext__(self):
        msg = await self.fetch_data()
        if msg:
            return msg
        else:
            raise StopAsyncIteration

    async def close(self):
        self._closed = True
        if self._conn:
            await self._conn.close()
            self._conn = None

    async def fetch_data(self):
        if not self._response_queue.empty():
            message = self._response_queue.get_nowait()
        else:
            asyncio.ensure_future(self._parse_data(), loop=self._loop)
            message = await self._response_queue.get()
        return message

    async def _parse_data(self):
        data = await self._ws.receive()
        # parse aiohttp response here
        message = json.loads(data.data.decode("utf-8"))
        message = Message(message["status"]["code"],
                          message["result"]["data"],
                          message["status"]["message"],
                          message["result"]["meta"])
        if message.status_code in [200, 206, 204]:
            self._response_queue.put_nowait(message)
            if message.status_code != 206:
                await self.term()
                self._response_queue.put_nowait(None)
        elif message.status_code == 407:
            self._authenticate(self._username, self._password,
                               self._processor, self._session)
            message = await self.fetch_data()
        else:
            await self.term()
            raise RuntimeError("{0} {1}".format(message.status_code,
                                                message.message))

    async def term(self):
        async with self._conn.conn_condition:
            self._conn.remove_inflight()
            self._conn.conn_condition.notify()
        self._closed = True
        if self._force_close:
            await self.close()
        elif self._force_reclaim:
            await self._conn.reclaim()

class Connection:

    def __init__(self, ws, loop, *, force_close=True, force_reclaim=False,
                 driver=None, username=None, password=None):
        self._ws = ws
        self._loop = loop
        self._force_close = force_close
        self._force_reclaim = force_reclaim
        self._driver = driver
        self._username = username
        self._password = password
        self._closed = False
        self._conn_condition = asyncio.Condition(loop=loop)
        self._inflight_messages = 0
        self._max_inflight_messages = 32

    @property
    def inflight_messages(self):
        return self._inflight_messages

    @property
    def max_inflight_messages(self):
        return self._max_inflight_messages

    @property
    def conn_condition(self):
        return self._conn_condition

    @property
    def closed(self):
        return self._closed

    @property
    def force_close(self):
        return self._force_close

    @property
    def force_reclaim(self):
        return self._force_reclaim

    @property
    def driver(self):
        return self._driver

    def add_inflight(self):
        self._inflight_messages += 1

    def remove_inflight(self):
        self._inflight_messages -= 1

    async def reclaim(self):
        if self.driver:
            await self.driver.reclaim(self)

    async def submit(self,
                    gremlin,
                    *,
                    bindings=None,
                    lang='gremlin-groovy',
                    aliases=None,
                    op="eval",
                    processor="",
                    session=None,
                    request_id=None):
        if aliases is None:
            aliases = {}
        message = self._prepare_message(gremlin,
                                        bindings,
                                        lang,
                                        aliases,
                                        op,
                                        processor,
                                        session,
                                        request_id)
        async with self.conn_condition:
            while True:
                if self.inflight_messages < self.max_inflight_messages:
                    self._ws.send_bytes(message)
                    return AsyncResponseIter(self._ws, self._loop, self,
                                             self._username, self._password,
                                             processor, session)
                else:
                    await self.conn_condition.wait()

    async def close(self):
        async with self.conn_condition:
            await self._ws.close()
            self._closed = True
            self.driver._open_connections -= 1
            self._driver = None

    def _prepare_message(self, gremlin, bindings, lang, aliases, op, processor,
                         session, request_id):
        if request_id is None:
            request_id = str(uuid.uuid4())
        message = {
            "requestId": request_id,
            "op": op,
            "processor": processor,
            "args": {
                "gremlin": gremlin,
                "bindings": bindings,
                "language":  lang,
                "aliases": aliases
            }
        }
        message = self._finalize_message(message, processor, session)
        return message

    def _authenticate(self, username, password, processor, session):
        auth = b"".join([b"\x00", username.encode("utf-8"),
                         b"\x00", password.encode("utf-8")])
        message = {
            "requestId": str(uuid.uuid4()),
            "op": "authentication",
            "processor": "",
            "args": {
                "sasl": base64.b64encode(auth).decode()
            }
        }
        message = self._finalize_message(message, processor, session)
        self._ws.submit(message, binary=True)

    def _finalize_message(self, message, processor, session):
        if processor == "session":
            if session is None:
                raise RuntimeError("session processor requires a session id")
            else:
                message["args"].update({"session": session})
        message = json.dumps(message)
        return self._set_message_header(message, "application/json")

    @staticmethod
    def _set_message_header(message, mime_type):
        if mime_type == "application/json":
            mime_len = b"\x10"
            mime_type = b"application/json"
        else:
            raise ValueError("Unknown mime type.")
        return b"".join([mime_len, mime_type, message.encode("utf-8")])
