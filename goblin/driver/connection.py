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


class Response:

    def __init__(self, response_queue, loop):
        self._response_queue = response_queue
        self._loop = loop
        self._done = False

    async def __aiter__(self):
        return self

    async def __anext__(self):
        msg = await self.fetch_data()
        if msg:
            return msg
        else:
            raise StopAsyncIteration

    async def fetch_data(self):
        if self._done:
            return None
        msg = await self._response_queue.get()
        if msg is None:
            self._done = True
        return msg


class AbstractConnection(abc.ABC):

    @abc.abstractmethod
    async def submit(self):
        raise NotImplementedError

    @abc.abstractmethod
    async def close(self):
        raise NotImplementedError


class Connection(AbstractConnection):

    def __init__(self, ws, loop, conn_factory, *, force_close=True,
                 force_release=False, pool=None, username=None,
                 password=None):
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

    @property
    def response_queues(self):
        return self._response_queues

    @property
    def closed(self):
        return self._closed

    @property
    def force_close(self):
        return self._force_close

    @property
    def force_release(self):
        return self._force_release

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
        response_queue = asyncio.Queue(loop=self._loop)
        self.response_queues[request_id] = response_queue
        self._ws.send_bytes(message)
        self._loop.create_task(self.receive())
        return Response(response_queue, self._loop)

    async def close(self):
        await self._ws.close()
        self._closed = True
        self._pool = None
        await self._conn_factory.close()

    def _prepare_message(self, gremlin, bindings, lang, aliases, op,
                         processor, session, request_id):
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

    async def receive(self):
        data = await self._ws.receive()
        # parse aiohttp response here
        message = json.loads(data.data.decode("utf-8"))
        request_id = message['requestId']
        message = Message(message["status"]["code"],
                          message["result"]["data"],
                          message["status"]["message"],
                          message["result"]["meta"])
        response_queue = self._response_queues[request_id]
        if message.status_code in [200, 206, 204]:
            response_queue.put_nowait(message)
            if message.status_code == 206:
                self._loop.create_task(self.receive())
            else:
                response_queue.put_nowait(None)
                del self._response_queues[request_id]
        elif message.status_code == 407:
            self._authenticate(self._username, self._password,
                               self._processor, self._session)
            self._loop.create_task(self.receive())
        else:
            del self._response_queues[request_id]
            raise RuntimeError("{0} {1}".format(message.status_code,
                                                message.message))

    async def term(self):
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
