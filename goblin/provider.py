class Provider:
    """Superclass for provider plugins"""
    DEFAULT_OP_ARGS = {}

    @classmethod
    def get_default_op_args(cls, processor):
        return cls.DEFAULT_OP_ARGS.get(processor, dict())


class TinkerGraph(Provider):  # TODO
    """Default provider"""

    @staticmethod
    def get_hashable_id(val):
        return val
