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

try:
    import ujson as json
except ImportError:
    import json

from gremlin_python.process.traversal import Bytecode, Traverser
from gremlin_python.process.translator import GroovyTranslator
from gremlin_python.structure.io.graphson import GraphSONWriter, GraphSONReader


class Processor:
    """Base class for OpProcessor serialization system."""

    def get_op(self, op):
        op = getattr(self, op, None)
        if not op:
            raise Exception("Processor does not support op")
        return op


class GraphSONMessageSerializer:
    """Message serializer for GraphSONv1"""
    # processors and ops
    class standard(Processor):

        def authentication(self, args):
            return args

        def eval(self, args):
            gremlin = args['gremlin']
            if isinstance(gremlin, Bytecode):
                translator = GroovyTranslator('g')
                args['gremlin'] = translator.translate(gremlin)
                args['bindings'] = gremlin.bindings
            return args


    class session(standard):
        pass


    def get_processor(self, processor):
        processor = getattr(self, processor, None)
        if not processor:
            raise Exception("Unknown processor")
        return processor()

    def serialize_message(self, request_id, processor, op, **args):
        if not processor:
            processor_obj = self.get_processor('standard')
        else:
            processor_obj = self.get_processor(processor)
        op_method = processor_obj.get_op(op)
        args = op_method(args)
        message = self.build_message(request_id, processor, op, args)
        return message

    def build_message(self, request_id, processor, op, args):
        message = {
            'requestId': request_id,
            'processor': processor,
            'op': op,
            'args': args
        }
        return self.finalize_message(message,  b'\x10', b'application/json')

    def finalize_message(self, message, mime_len, mime_type):
        message = json.dumps(message)
        message = b''.join([mime_len, mime_type, message.encode('utf-8')])
        return message

    def deserialize_message(self, message):
        return Traverser(message)


class GraphSON2MessageSerializer(GraphSONMessageSerializer):
    """Message serializer for GraphSONv2"""

    class session(GraphSONMessageSerializer.session):

        def close(self, args):
            return args


    class traversal(Processor):

        def authentication(self, args):
            return args

        def bytecode(self, args):
            gremlin = args['gremlin']
            args['gremlin'] = GraphSONWriter.writeObject(gremlin)
            aliases = args.get('aliases', '')
            if not aliases:
                aliases = {'g': 'g'}
            args['aliases'] = aliases
            return args

        def close(self, args):
            return self.keys(args)

        def gather(self, args):
            side_effect = args['sideEffect']
            args['sideEffect'] = {'@type': 'g:UUID', '@value': side_effect}
            aliases = args.get('aliases', '')
            if not aliases:
                aliases = {'g': 'g'}
            args['aliases'] = aliases
            return args

        def keys(self, args):
            side_effect = args['sideEffect']
            args['sideEffect'] = {'@type': 'g:UUID', '@value': side_effect}
            return args

    def build_message(self, request_id, processor, op, args):
        message = {
            'requestId': {'@type': 'g:UUID', '@value': request_id},
            'processor': processor,
            'op': op,
            'args': args
        }
        return self.finalize_message(message, b"\x21",
                                     b"application/vnd.gremlin-v2.0+json")

    def deserialize_message(self, message):
        if isinstance(message, dict):
            if message.get('@type', '') == 'g:Traverser':
                obj = GraphSONReader._objectify(message)
            else:
                obj = Traverser(message.get('@value', message))
        else:
            obj = Traverser(message)
        return obj
