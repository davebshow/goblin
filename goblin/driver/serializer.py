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

import json


class GraphSONMessageSerializer:

    def serialize_message(self, request_id, processor, op, **args):
        message = {
            'requestId': request_id,
            'processor': processor,
            'op': op,
            'args': args
        }
        message = json.dumps(message)
        mime_len = b'\x10'
        mime_type = b'application/json'
        message = b''.join([mime_len, mime_type, message.encode('utf-8')])
        return message
