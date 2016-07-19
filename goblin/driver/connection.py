# Copyright 2016 ZEROFAIL
#
# This file is part of Goblin.
#
# Goblin is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Goblin is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with Goblin.  If not, see <http://www.gnu.org/licenses/>.

import abc
import asyncio
import collections
import functools
import json
import logging
import uuid

import aiohttp

from goblin import exception


logger = logging.getLogger(__name__)


Message = collections.namedtuple(
    "Message",
    ["status_code", "data", "message"])


def error_handler(fn):
    @functools.wraps(fn)
    async def wrapper(self):
        msg = await fn(self)
        if msg:
            if msg.status_code not in [200, 206, 204]:
                raise exception.GremlinServerError(
                    "{0}: {1}".format(msg.status_code, msg.message))
            msg = msg.data
        return msg
    return wrapper


class Response:
    """Gremlin Server response implementated as an async iterator."""
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

    @error_handler
    async def fetch_data(self):
        """Get a single message from the response stream"""
        if self._done:
            return None
        msg = await self._response_queue.get()
        if msg is None:
            self._done = True
        return msg


class AbstractConnection(abc.ABC):
    """Defines the interface for a connection object."""
    @abc.abstractmethod
    async def submit(self):
        raise NotImplementedError

    @abc.abstractmethod
    async def close(self):
        raise NotImplementedError


class Connection(AbstractConnection):
    """
    Main classd for interacting with the Gremlin Server. Encapsulates a
    websocket connection. Not instantiated directly. Instead use
    :py:meth:`GremlinServer.open<goblin.driver.api.GremlinServer.open>`.
    """
    def __init__(self, url, ws, loop, conn_factory, *, username=None,
                 password=None):
        self._url = url
        self._ws = ws
        self._loop = loop
        self._conn_factory = conn_factory
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
    def url(self):
        return self._url

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
        """
        Submit a script and bindings to the Gremlin Server

        :param str gremlin: Gremlin script to submit to server.
        :param dict bindings: A mapping of bindings for Gremlin script.
        :param str lang: Language of scripts submitted to the server.
            "gremlin-groovy" by default
        :param dict aliases: Rebind ``Graph`` and ``TraversalSource``
            objects to different variable names in the current request
        :param str op: Gremlin Server op argument. "eval" by default.
        :param str processor: Gremlin Server processor argument. "" by default.
        :param str session: Session id (optional). Typically a uuid
        :param str request_id: Request id (optional). Typically a uuid

        :returns: :py:class:`Response` object
        """
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
        if self._ws.closed:
            self._ws = await self.conn_factory.ws_connect(self.url)
        self._ws.send_bytes(message)
        self._loop.create_task(self._receive())
        return Response(response_queue, self._loop)

    async def close(self):
        """Close underlying connection and mark as closed."""
        await self._ws.close()
        self._closed = True
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

    async def _receive(self):
        data = await self._ws.receive()
        if data.tp == aiohttp.MsgType.close:
            await self._ws.close()
        elif data.tp == aiohttp.MsgType.error:
            raise data.data
        elif data.tp == aiohttp.MsgType.closed:
            pass
        else:
            if data.tp == aiohttp.MsgType.binary:
                data = data.data.decode()
            elif data.tp == aiohttp.MsgType.text:
                data = data.strip()
            message = json.loads(data)
            request_id = message['requestId']
            status_code = message['status']['code']
            data = message["result"]["data"]
            msg = message["status"]["message"]
            response_queue = self._response_queues[request_id]
            if status_code == 407:
                await self._authenticate(self._username, self._password,
                                         self._processor, self._session)
                self._loop.create_task(self._receive())
            else:
                if data:
                    for result in data:
                        message = Message(status_code, result, msg)
                        response_queue.put_nowait(message)
                else:
                    message = Message(status_code, data, msg)
                    response_queue.put_nowait(message)
                if status_code == 206:
                    self._loop.create_task(self._receive())
                else:
                    response_queue.put_nowait(None)
                    del self._response_queues[request_id]

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()
        self._conn = None
