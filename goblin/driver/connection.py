import abc
import asyncio
import collections
import json
import logging
import uuid


logger = logging.getLogger(__name__)


Message = collections.namedtuple(
    "Message",
    ["status_code", "data", "message", "metadata"])


class AsyncResponseIter:

    def __init__(self, response_queue, loop, conn, username, password,
                 processor, session):
        self._response_queue = response_queue
        self._loop = loop
        self._conn = conn
        self._force_close = self._conn.force_close
        self._force_release = self._conn.force_release

    async def __aiter__(self):
        return self

    async def __anext__(self):
        msg = await self.fetch_data()
        if msg:
            return msg
        else:
            raise StopAsyncIteration

    async def fetch_data(self):
        if not self._response_queue.empty():
            message = self._response_queue.get_nowait()
        else:
            self._loop.create_task(self._conn.get_data())
            message = await self._response_queue.get()
        return message

    async def close(self):
        if self._conn:
            await self._conn.close()
            self._conn = None


class AbstractConnection(abc.ABC):

    @abc.abstractmethod
    async def submit(self):
        raise NotImplementedError

    @abc.abstractmethod
    async def close(self):
        raise NotImplementedError

    @abc.abstractproperty
    def closed(self):
        return self._closed

    @abc.abstractproperty
    def force_close(self):
        return self._force_close

    @abc.abstractproperty
    def force_release(self):
        return self._force_release


class Connection(AbstractConnection):

    def __init__(self, ws, loop, conn_factory, *, max_inflight=None,
                 force_close=True, force_release=False,
                 pool=None, username=None, password=None):
        self._ws = ws
        self._loop = loop
        self._conn_factory = conn_factory
        self._force_close = force_close
        self._force_release = force_release
        self._pool = pool
        self._username = username
        self._password = password
        self._closed = False
        self._response_queues = {}
        self._inflight = 0
        if not max_inflight:
            max_inflight = 32
        self._max_inflight = 32
        self._semaphore = asyncio.Semaphore(self._max_inflight,
                                            loop=self._loop)

    @property
    def max_inflight(self):
        return self._max_inflight

    @property
    def max_inflight(self):
        return self._max_inflight

    def remove_inflight(self):
        self._inflight -= 1

    @property
    def response_queues(self):
        return self._response_queues

    @property
    def semaphore(self):
        return self._semaphore

    @property
    def closed(self):
        return super().close

    @property
    def force_close(self):
        return super().force_close

    @property
    def force_release(self):
        return super().force_release

    async def release(self):
        if self.pool:
            await self.pool.release(self)

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
        if request_id is None:
            request_id = str(uuid.uuid4())
        message = self._prepare_message(gremlin,
                                        bindings,
                                        lang,
                                        aliases,
                                        op,
                                        processor,
                                        session,
                                        request_id)
        await self.semaphore.acquire()
        self._inflight += 1
        response_queue = asyncio.Queue(loop=self._loop)
        self.response_queues[request_id] = response_queue
        self._ws.send_bytes(message)
        return AsyncResponseIter(response_queue, self._loop, self,
                                 self._username, self._password,
                                 processor, session)

    async def close(self):
        await self._ws.close()
        self._closed = True
        self._pool = None
        await self._conn_factory.close()

    def _prepare_message(self, gremlin, bindings, lang, aliases, op, processor,
                         session, request_id):
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

    async def get_data(self):
        data = await self._ws.receive()
        # parse aiohttp response here
        message = json.loads(data.data.decode("utf-8"))
        request_id = message['requestId']
        message = Message(message["status"]["code"],
                          message["result"]["data"],
                          message["status"]["message"],
                          message["result"]["meta"])
        if message.status_code in [200, 206, 204]:
            response_queue = self.response_queues[request_id]
            response_queue.put_nowait(message)
            if message.status_code != 206:
                await self.term()
                response_queue.put_nowait(None)
        elif message.status_code == 407:
            self._authenticate(self._username, self._password,
                               self._processor, self._session)
            message = await self.fetch_data()
        else:
            await self.term()
            raise RuntimeError("{0} {1}".format(message.status_code,
                                                message.message))

    async def term(self):
        self.remove_inflight()
        self.semaphore.release()
        if self._force_close:
            await self.close()
        elif self._force_release:
            await self.release()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()
        self._conn = None
        self._pool = None
