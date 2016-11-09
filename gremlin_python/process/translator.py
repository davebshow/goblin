'''
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
'''

# Translate bytecode. Used for backwards compatiblitity

from abc import abstractmethod
from aenum import Enum

from gremlin_python.statics import long
from gremlin_python.process.traversal import P, Bytecode, Binding


class RawExpression(object):
   def __init__(self, *args):
      self.bindings = dict()
      self.parts = [self._process_arg(arg) for arg in args]

   def _process_arg(self, arg):
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
         self.bindings[arg[0]] = arg[1]
         return Raw(arg[0])
      else:
         return Raw(arg)

class Raw(object):
   def __init__(self, value):
      self.value = value

   def __str__(self):
      return str(self.value)


TO_JAVA_MAP = {"_global": "global", "_as": "as", "_in": "in", "_and": "and",
               "_or": "or", "_is": "is", "_not": "not", "_from": "from",
               "Cardinality": "VertexProperty.Cardinality", "Barrier": "SackFunctions.Barrier"}


class Translator(object):
    def __init__(self, traversal_source, anonymous_traversal, target_language):
        self.traversal_source = traversal_source
        self.anonymous_traversal = anonymous_traversal
        self.target_language = target_language

    @abstractmethod
    def translate(self, bytecode):
        return

    @abstractmethod
    def __repr__(self):
        return "translator[" + self.traversal_source + ":" + self.target_language + "]"


class SymbolHelper(object):
    @staticmethod
    def toJava(symbol):
        if (symbol in TO_JAVA_MAP):
            return TO_JAVA_MAP[symbol]
        else:
            return symbol

    @staticmethod
    def mapEnum(enum):
        if (enum in enumMap):
            return enumMap[enum]
        else:
            return enum


class GroovyTranslator(Translator):
    def __init__(self, traversal_source, anonymous_traversal="__", target_language="gremlin-groovy"):
        Translator.__init__(self, traversal_source, anonymous_traversal, target_language)

    def translate(self, bytecode):
        return self._internalTranslate(self.traversal_source, bytecode)

    def _internalTranslate(self, start, bytecode):
        traversal_script = start
        for instruction in bytecode.source_instructions:
            traversal_script = traversal_script + "." + SymbolHelper.toJava(
                instruction[0]) + "(" + self.stringify(*instruction[1:]) + ")"
        for instruction in bytecode.step_instructions:
            traversal_script = traversal_script + "." + SymbolHelper.toJava(
                instruction[0]) + "(" + self.stringify(*instruction[1:]) + ")"
        return traversal_script

    def stringOrObject(self, arg):
        if isinstance(arg, str):
            return "\"" + arg + "\""
        elif isinstance(arg, bool):
            return str(arg).lower()
        elif isinstance(arg, long):
            return str(arg) + "L"
        elif isinstance(arg, int):
            return str(arg)
        elif isinstance(arg, float):
            return str(arg) + "f"
        elif isinstance(arg, Enum):  # Column, Order, Direction, Scope, T, etc.
            return SymbolHelper.toJava(type(arg).__name__) + "." + SymbolHelper.toJava(str(arg.name))
        elif isinstance(arg, P):
            if arg.other is None:
                return "P." + SymbolHelper.toJava(arg.operator) + "(" + self.stringOrObject(
                    arg.value) + ")"
            else:
                return self.stringOrObject(arg.other) + "." + SymbolHelper.toJava(
                    arg.operator) + "(" + self.stringOrObject(arg.value) + ")"
        elif isinstance(arg, Binding):
            return arg.key
        elif isinstance(arg, Bytecode):
            return self._internalTranslate(self.anonymous_traversal, arg)
        elif callable(arg):  # closures
            lambdaString = arg().strip()
            if lambdaString.startswith("{"):
                return lambdaString
            else:
                return "{" + lambdaString + "}"
        elif isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):  # bindings
            return arg[0]
        elif isinstance(arg, RawExpression):
            return "".join(self.stringOrObject(i) for i in arg.parts)
        else:
            return str(arg)

    def stringify(self, *args):
        if len(args) == 0:
            return ""
        elif len(args) == 1:
            return self.stringOrObject(args[0])
        else:
            return ", ".join(self.stringOrObject(i) for i in args)
