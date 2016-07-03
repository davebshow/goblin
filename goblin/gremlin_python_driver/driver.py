import collections
import json
import uuid


import aiohttp


Message = collections.namedtuple(
    "Message",
    ["status_code", "data", "message", "metadata"])


def create_connection(url, loop):
    return Driver(url, loop)


class Driver:

    def __init__(self, url, loop):
        self._url = url
        self._loop = loop
        self._session = aiohttp.ClientSession(loop=self._loop)
        self._conn = None

    @property
    def conn(self):
        return self._conn

    async def __aenter__(self):
        conn = await self.connect()
        self._conn = conn
        return conn

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    async def connect(self):
        ws = await self._session.ws_connect(self._url)
        return Connection(ws, self._loop)

    async def close(self):
        if self._conn:
            await self._conn.close()
            self._conn = None
        await self._session.close()


class AsyncResponseIter:

    def __init__(self, ws, loop, conn):
        self._ws = ws
        self._loop = loop
        self._conn = conn
        self._force_close = self._conn.force_close
        self._closed = False

    async def __aiter__(self):
        return self

    async def __anext__(self):
        msg = await self.fetch_data()
        if msg:
            return msg
        else:
            if self._force_close:
                await self.close()
            raise StopAsyncIteration

    async def close(self):
        self._closed = True
        if self._conn:
            await self._conn.close()
            self._conn = None

    async def fetch_data(self):
        if self._closed:
            return
        data = await self._ws.receive()
        message = json.loads(data.data.decode("utf-8"))
        message = Message(message["status"]["code"],
                          message["result"]["data"],
                          message["status"]["message"],
                          message["result"]["meta"])
        if message.status_code != 206:
            self._closed = True
        return message


class Connection:

    def __init__(self, ws, loop, *, force_close=True):
        self._ws = ws
        self._loop = loop
        self._force_close = force_close

    @property
    def force_close(self):
        return self._force_close

    def submit(self,
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

        self._ws.send_bytes(message)
        return AsyncResponseIter(self._ws, self._loop, self)

    async def close(self):
        await self._ws.close()

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
        self.conn.send(message, binary=True)

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
